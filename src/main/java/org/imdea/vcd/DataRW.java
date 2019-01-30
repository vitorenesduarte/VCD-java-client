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
import java.util.Map;
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
            MessageSet ms = MessageSet.newBuilder()
                    .addMessages(message)
                    .setStatus(MessageSet.Status.START)
                    .build();
            doWrite(ms, this.out);
        }
    }

    private void doWrite(MessageSet messageSet, DataOutputStream o) throws IOException {
        byte[] data = messageSet.toByteArray();
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

                        for (MessageSet messageSet : Batch.pack(ops)) {
                            doWrite(messageSet, this.out);
                        }
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

    // socket reader -> client | queue runner
    // queue runner -> deliverer
    // deliverer -> client
    private class SocketReader extends Thread {

        private final Logger LOGGER = VCDLogger.init(SocketReader.class);

        private final DataInputStream in;
        private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
        private final LinkedBlockingQueue<Reply> toQueueRunner;
        private final Boolean batching;
        private final QueueRunner queueRunner;

        public SocketReader(DataInputStream in, LinkedBlockingQueue<Optional<MessageSet>> toClient, Config config) {
            this.in = in;
            this.toClient = toClient;
            this.toQueueRunner = new LinkedBlockingQueue<>();
            this.batching = batching(config.getBatchWait());
            this.queueRunner = new QueueRunner(this.toClient, this.toQueueRunner, config);
        }

        public void close() {
            this.queueRunner.close();
            this.queueRunner.interrupt();
        }

        @Override
        public void run() {
            // start queue runner
            this.queueRunner.start();

            try {
                try {
                    while (true) {
                        int length = in.readInt();
                        byte data[] = new byte[length];
                        in.readFully(data, 0, length);
                        Reply reply = Reply.parseFrom(data);

                        // if durable notification, send it to client
                        // otherwise to dep queue thread
                        switch (reply.getReplyCase()) {
                            case SET:
                                MessageSet durable = reply.getSet();
                                if (durable.getMessagesCount() != 1 || durable.getStatus() != MessageSet.Status.DURABLE) {
                                    LOGGER.log(Level.INFO, "Invalid durable notification");
                                }

                                if (this.batching) {
                                    toClient.put(Optional.of(Batch.unpack(durable)));
                                } else {
                                    toClient.put(Optional.of(durable));
                                }
                                break;
                            default:
                                if (reply.hasCommit()) {
                                    RWMetrics.startExecution(Dot.dot(reply.getCommit().getDot()));
                                }
                                toQueueRunner.put(reply);
                                break;
                        }
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

    private class QueueRunner extends Thread {

        private static final boolean RECORD_TRACE = false;

        private final Logger LOGGER = VCDLogger.init(QueueRunner.class);

        private final LinkedBlockingQueue<Reply> toQueueRunner;
        private final LinkedBlockingQueue<List<ConfQueueBox>> toDeliverer;
        private final Boolean batching;
        private final Boolean optDelivery;
        private final Deliverer deliverer;

        private ConfQueue queue;

        private Jedis jedis = null;
        private String jedisKey = null;

        public QueueRunner(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<Reply> toQueueRunner, Config config) {
            this.batching = batching(config.getBatchWait());
            this.optDelivery = config.getOptDelivery();
            this.toQueueRunner = toQueueRunner;
            this.toDeliverer = new LinkedBlockingQueue<>();
            this.deliverer = new Deliverer(toClient, this.toDeliverer);
            if (RECORD_TRACE) {
                connectToRedis(config);
            }
        }

        public void close() {
            this.deliverer.interrupt();
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
            // start deliverer
            this.deliverer.start();

            try {
                while (true) {
                    Reply reply = toQueueRunner.take();

                    switch (reply.getReplyCase()) {
                        case INIT:
                            // create committed clock
                            Clock<ExceptionSet> committed = Clock.eclock(reply.getInit().getCommittedMap());

                            // store trace in redis
                            if (RECORD_TRACE) {
                                pushToRedis(committed);
                            }

                            // create delivery queue
                            queue = new ConfQueue(committed, this.batching, this.optDelivery);
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

                            RWMetrics.endMidExecution(dot);

                            final Timer.Context queueAddContext = RWMetrics.QUEUE_ADD.time();
                            Map<Integer, List<Long>> missing = queue.add(dot, message, conf);
                            Integer missingCount = 0;
                            for(List<Long> s : missing.values()) {
                                missingCount += s.size();
                            }
                            RWMetrics.MISSING_DEPS.update(missingCount);
                            queueAddContext.stop();

                            List<ConfQueueBox> toDeliver = queue.getToDeliver();
                            if (!toDeliver.isEmpty()) {
                                toDeliverer.put(toDeliver);
                            }
                            RWMetrics.QUEUE_ELEMENTS.update(queue.elements());
                            break;

                        default:
                            throw new RuntimeException("Reply type not supported:" + reply.getReplyCase());
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
            try {
                while (true) {
                    List<ConfQueueBox> toDeliver = toDeliverer.take();
                    RWMetrics.COMPONENTS_COUNT.update(toDeliver.size());

                    final Timer.Context deliverLoopContext = RWMetrics.DELIVER_LOOP.time();
                    // create message set builder
                    MessageSet.Builder builder = MessageSet.newBuilder();

                    for (ConfQueueBox b : toDeliver) {
                        for (Dot d : b.getDots()) {
                            RWMetrics.endExecution(d);
                        }
                        // update message set builder
                        builder.addAllMessages(b.sortMessages());
                    }
                    // build message
                    builder.setStatus(MessageSet.Status.DELIVERED);
                    MessageSet messageSet = builder.build();

                    // send it to client
                    toClient.put(Optional.of(messageSet));
                    deliverLoopContext.stop();
                }
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }
}
