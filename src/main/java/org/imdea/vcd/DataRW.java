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

    private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
    private final SocketReader socketReader;

    public DataRW(DataInputStream in, DataOutputStream out, Config config) {
        this.in = in;
        this.out = out;
        this.toClient = new LinkedBlockingQueue<>();
        this.socketReader = new SocketReader(this.in, this.toClient, config);
    }

    private boolean batching(Integer batchWait) {
        return batchWait > 0;
    }

    public void start() {
        this.socketReader.start();
    }

    public void close() throws IOException {
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
        doWrite(Batch.pack(message), this.out);
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

                        // if commit, send notification to client
                        // and forward it to dep queue thread
                        if (reply.hasCommit()) {
                            Commit commit = reply.getCommit();
                            RWMetrics.startExecution(Dot.dot(commit.getDot()));
                            toClient.put(Optional.of(Batch.unpack(commit.getMessage(), MessageSet.Status.COMMIT)));
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
        private final LinkedBlockingQueue<QueueRunnerMsg> toQueueRunner;
        private final QueueRunner queueRunner;

        private Jedis jedis = null;
        private String jedisKey = null;

        public Parser(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<Reply> toParser, Config config) {
            this.toParser = toParser;
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
                    Reply reply = toParser.take();

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
            } catch (InterruptedException e) {
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
                    QueueRunnerMsg msg = toQueueRunner.take();

                    if (msg.isInit) {
                        // create delivery queue
                        queue = new ConfQueue(msg.committed, this.batching, this.optDelivery);
                    } else {
                        // add to delivery queue
                        RWMetrics.endMidExecution(msg.dot);

                        final Timer.Context queueAddContext = RWMetrics.QUEUE_ADD.time();
                        queue.add(msg.dot, msg.message, msg.conf);
                        queueAddContext.stop();

                        List<ConfQueueBox> toDeliver = queue.getToDeliver();
                        if (!toDeliver.isEmpty()) {
                            toDeliverer.put(toDeliver);
                        }
                        RWMetrics.QUEUE_ELEMENTS.update(queue.elements());
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
                    List<ConfQueueBox> toDeliver = toDeliverer.take();
                    RWMetrics.COMPONENTS_COUNT.update(toDeliver.size());

                    final Timer.Context deliverLoopContext = RWMetrics.DELIVER_LOOP.time();

                    for (ConfQueueBox b : toDeliver) {
                        for (Dot d : b.getDots()) {
                            RWMetrics.endExecution(d);
                        }

                        // create message set builder
                        MessageSet.Builder builder = MessageSet.newBuilder();

                        // update message set builder
                        builder.addAllMessages(b.sortMessages());

                        // build message
                        builder.setStatus(MessageSet.Status.DELIVERED);
                        MessageSet messageSet = builder.build();

                        // send it to client
                        toClient.put(Optional.of(messageSet));
                    }

                    deliverLoopContext.stop();
                }
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }
}
