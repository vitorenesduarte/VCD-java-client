package org.imdea.vcd;

import com.codahale.metrics.Timer;
import com.google.protobuf.InvalidProtocolBufferException;
import org.imdea.vcd.metrics.RWMetrics;
import org.imdea.vcd.pb.Proto.Commit;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;
import org.imdea.vcd.pb.Proto.Reply;
import org.imdea.vcd.queue.ConfQueue;
import org.imdea.vcd.queue.ConfQueueBox;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.MaxInt;
import org.imdea.vcd.util.Batch;
import org.imdea.vcd.util.Trace;
import redis.clients.jedis.Jedis;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Vitor Enes
 */
public class DataRW {

    private final DataInputStream in;
    private final DataOutputStream out;

    private final LinkedBlockingQueue<Message> toWriter;
    private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
    private final Boolean batching;

    private final Writer writer;
    private final SocketReader socketReader;

    public DataRW(DataInputStream in, DataOutputStream out, Config config) {
        this.in = in;
        this.out = out;
        this.toWriter = new LinkedBlockingQueue<>();
        this.toClient = new LinkedBlockingQueue<>();
        this.batching = batching(config.getBatchWait());
        this.writer = new Writer(this.out, this.toWriter, config);
        this.socketReader = new SocketReader(this.in, this.toClient, config);
    }

    private boolean batching(Integer batchWait) {
        return batchWait > 0;
    }

    public void start() {
        if (this.batching) {
            this.writer.start();
        }
        this.socketReader.start();
    }

    public void close() throws IOException {
        if (this.batching) {
            this.writer.interrupt();
        }
        this.socketReader.close();
        this.socketReader.interrupt();
        this.in.close();
        this.out.close();
    }

    public MessageSet read() throws IOException, InterruptedException {
        Optional<MessageSet> result = this.toClient.take();
        if (!result.isPresent()) {
            throw new IOException();
        }
        return result.get();
    }

    public void write(Message message) throws IOException, InterruptedException {
        if (this.batching) {
            toWriter.put(message);
        } else {
            doWrite(Batch.pack(Arrays.asList(message)), this.out);
        }
    }

    private void doWrite(Message message, DataOutputStream o) throws IOException {
        byte[] data = message.toByteArray();
        o.writeInt(data.length);
        o.write(data, 0, data.length);
        o.flush();
    }

    private void notifyFailureToClient(LinkedBlockingQueue<Optional<MessageSet>> toClient) throws InterruptedException {
        toClient.put(Optional.empty());
    }

    private class Writer extends Thread {

        private final Logger LOGGER = VCDLogger.init(Writer.class);

        private final LinkedBlockingQueue<Message> toWriter;
        private final DataOutputStream out;
        private final Integer batchWait;

        public Writer(DataOutputStream out, LinkedBlockingQueue<Message> toWriter, Config config) {
            this.out = out;
            this.toWriter = toWriter;
            this.batchWait = config.getBatchWait();
        }

        @Override
        public void run() {
            LOGGER.log(Level.INFO, "Writer thread started...");
            try {
                try {
                    while (true) {
                        Message first = toWriter.take();
                        List<Message> ops = new ArrayList<>();

                        // wait, and drain the queue
                        Thread.sleep(this.batchWait);
                        toWriter.drainTo(ops);

                        ops.add(first);

                        // batch size metrics
                        RWMetrics.BATCH_SIZE.update(ops.size());

                        doWrite(Batch.pack(ops), this.out);
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, e.toString(), e);
                    notifyFailureToClient(toClient);
                }
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }

    // socket reader -> client | parser
    // parser -> queue runner
    // queue runner -> deliverer
    // deliverer -> client
    private class SocketReader extends Thread {

        private final Logger LOGGER = VCDLogger.init(SocketReader.class);

        private final DataInputStream in;
        private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
        private final LinkedBlockingQueue<Reply> toParser;
        private final Parser parser;

        public SocketReader(DataInputStream in, LinkedBlockingQueue<Optional<MessageSet>> toClient, Config config) {
            this.in = in;
            this.toClient = toClient;
            this.toParser = new LinkedBlockingQueue<>();
            this.parser = new Parser(this.toClient, this.toParser, config);
        }

        public void close() {
            this.parser.close();
            this.parser.interrupt();
        }

        @Override
        public void run() {
            // start parser
            this.parser.start();

            LOGGER.log(Level.INFO, "SocketReader thread started...");

            try {
                try {
                    while (true) {
                        int length = in.readInt();
                        byte data[] = new byte[length];
                        in.readFully(data, 0, length);
                        Reply reply = Reply.parseFrom(data);

                        if (reply.hasCommit()) {
                            // start execution
                            Dot dot = Dot.dot(reply.getCommit().getDot());
                            RWMetrics.startExecution(dot);
                        }

                        toParser.put(reply);
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, e.toString(), e);
                    notifyFailureToClient(toClient);
                }
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }

    private class Parser extends Thread {

        private static final boolean RECORD_TRACE = false;

        private final Logger LOGGER = VCDLogger.init(Parser.class);

        private final LinkedBlockingQueue<Reply> toParser;
        private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
        private final LinkedBlockingQueue<QueueRunnerMsg> toQueueRunner;
        private final QueueRunner queueRunner;

        private Jedis jedis = null;
        private String jedisKey = null;

        public Parser(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<Reply> toParser, Config config) {
            this.toParser = toParser;
            this.toClient = toClient;
            this.toQueueRunner = new LinkedBlockingQueue<>();
            this.queueRunner = new QueueRunner(toClient, this.toQueueRunner, config);
            if (RECORD_TRACE) {
                connectToRedis(config);
            }
        }

        public void close() {
            this.queueRunner.close();
            this.queueRunner.interrupt();
        }

        private void connectToRedis(Config config) {
            this.jedis = (config.getRedis() != null) ? new Jedis(config.getRedis()) : null;
            this.jedisKey = "" + config.getNodeNumber() + "/" + "trace-" + config.getCluster();
        }

        private void pushToRedis(Clock<ExceptionSet> committed) {
            String encoded = Trace.encode(committed);
            if (this.jedis != null) {
                jedis.rpush(jedisKey, encoded);
            }
        }

        private void pushToRedis(Dot dot, Clock<MaxInt> conf) {
            String encoded = Trace.encode(dot, conf);
            if (this.jedis != null) {
                jedis.rpush(jedisKey, encoded);
            }
        }

        @Override
        public void run() {
            // start queue runner
            this.queueRunner.start();

            LOGGER.log(Level.INFO, "Parser thread started...");

            try {
                while (true) {
                    List<Reply> msgs = new ArrayList<>();
                    toParser.drainTo(msgs);

                    for (Reply reply : msgs) {
                        switch (reply.getReplyCase()) {
                            case INIT:
                                // create committed clock
                                Clock<ExceptionSet> committed = Clock.eclock(reply.getInit().getCommittedMap());

                                // store trace in redis
                                if (RECORD_TRACE) {
                                    pushToRedis(committed);
                                }

                                // send to queue runner
                                QueueRunnerMsg initMsg = new QueueRunnerMsg(committed);
                                toQueueRunner.add(initMsg);
                                break;

                            case COMMIT:
                                final Timer.Context parseContext = RWMetrics.PARSE.time();

                                // fetch dot, dep, message and conf
                                Commit commit = reply.getCommit();
                                Dot dot = Dot.dot(commit.getDot());
                                Message message = commit.getMessage();
                                Clock<MaxInt> conf = Clock.vclock(commit.getConfMap());

                                parseContext.stop();

                                // if commit, send notification to client
                                // and forward it to dep queue thread
                                toClient.put(Optional.of(Batch.unpack(commit.getMessage(), MessageSet.Status.COMMIT)));
                                RWMetrics.endExecution0(dot);

                                // store trace in redis
                                if (RECORD_TRACE) {
                                    pushToRedis(dot, conf);
                                }

                                // send to queue runner
                                QueueRunnerMsg commitMsg = new QueueRunnerMsg(dot, message, conf);
                                toQueueRunner.add(commitMsg);
                                break;

                            default:
                                throw new RuntimeException("Reply type not supported:" + reply.getReplyCase());
                        }
                    }
                }
            } catch (InterruptedException | InvalidProtocolBufferException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }

    private class QueueRunnerMsg {

        private final boolean isInit;
        private Clock<ExceptionSet> committed;
        private Dot dot;
        private Message message;
        private Clock<MaxInt> conf;

        QueueRunnerMsg(Clock<ExceptionSet> committed) {
            this.isInit = true;
            this.committed = committed;
        }

        QueueRunnerMsg(Dot dot, Message message, Clock<MaxInt> conf) {
            this.isInit = false;
            this.dot = dot;
            this.message = message;
            this.conf = conf;
        }
    }

    private class QueueRunner extends Thread {

        private final Logger LOGGER = VCDLogger.init(QueueRunner.class);

        private final LinkedBlockingQueue<QueueRunnerMsg> toQueueRunner;
        private final LinkedBlockingQueue<List<ConfQueueBox>> toDeliverer;
        private final Boolean batching;
        private final Boolean optDelivery;
        private final Deliverer deliverer;

        private ConfQueue queue;

        public QueueRunner(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<QueueRunnerMsg> toQueueRunner, Config config) {
            this.batching = batching(config.getBatchWait());
            this.optDelivery = config.getOptDelivery();
            this.toQueueRunner = toQueueRunner;
            this.toDeliverer = new LinkedBlockingQueue<>();
            this.deliverer = new Deliverer(toClient, this.toDeliverer);
        }

        public void close() {
            this.deliverer.interrupt();
        }

        @Override
        public void run() {
            // start deliverer
            this.deliverer.start();

            LOGGER.log(Level.INFO, "QueueRunner thread started...");

            try {
                while (true) {
                    List<QueueRunnerMsg> msgs = new ArrayList<>();
                    toQueueRunner.drainTo(msgs);

                    for (QueueRunnerMsg msg : msgs) {
                        if (msg.isInit) {
                            // create delivery queue
                            queue = new ConfQueue(msg.committed, this.batching, this.optDelivery);
                        } else {
                            // add to delivery queue
                            RWMetrics.endExecution1(msg.dot);

                            final Timer.Context queueAddContext = RWMetrics.QUEUE_ADD.time();
                            queue.add(msg.dot, msg.message, msg.conf);
                            queueAddContext.stop();

                            final Timer.Context toDeliverContext = RWMetrics.TO_DELIVER.time();
                            List<ConfQueueBox> toDeliver = queue.getToDeliver();
                            if (!toDeliver.isEmpty()) {
                                toDeliverer.put(toDeliver);
                            }
                            toDeliverContext.stop();

                            RWMetrics.QUEUE_ELEMENTS.update(queue.elements());
                        }
                    }
                }
            } catch (InterruptedException | InvalidProtocolBufferException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }

    private class Deliverer extends Thread {

        private final Logger LOGGER = VCDLogger.init(Deliverer.class);
        private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
        private final LinkedBlockingQueue<List<ConfQueueBox>> toDeliverer;

        public Deliverer(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<List<ConfQueueBox>> toDeliverer) {
            this.toClient = toClient;
            this.toDeliverer = toDeliverer;
        }

        @Override
        public void run() {
            LOGGER.log(Level.INFO, "Deliverer thread started...");
            try {
                while (true) {
                    List<List<ConfQueueBox>> msgs = new ArrayList<>();
                    toDeliverer.drainTo(msgs);

                    for (List<ConfQueueBox> toDeliver : msgs) {

                        final Timer.Context deliverLoopContext = RWMetrics.DELIVER_LOOP.time();

                        for (ConfQueueBox b : toDeliver) {
                            for (Dot d : b.getDots()) {
                                RWMetrics.endExecution2(d);
                            }

                            // create message set builder
                            MessageSet.Builder builder = MessageSet.newBuilder();

                            // update message set builder
                            for (Message m : b.sortMessages()) {
                                builder.addAllMessages(Batch.unpack(m));
                            }
                            // build message
                            builder.setStatus(MessageSet.Status.DELIVERED);
                            MessageSet messageSet = builder.build();

                            // send it to client
                            toClient.put(Optional.of(messageSet));
                        }

                        deliverLoopContext.stop();
                    }
                }
            } catch (InterruptedException | InvalidProtocolBufferException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }
}
