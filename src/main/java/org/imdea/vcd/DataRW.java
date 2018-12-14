package org.imdea.vcd;

import org.imdea.vcd.util.Trace;
import org.imdea.vcd.util.Batch;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.protobuf.InvalidProtocolBufferException;
import org.imdea.vcd.pb.Proto.Commit;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;
import org.imdea.vcd.pb.Proto.Reply;
import org.imdea.vcd.queue.ConfQueueBox;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.MaxInt;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.imdea.vcd.queue.ConfQueue;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Vitor Enes
 */
public class DataRW {

    private static final MetricRegistry METRICS = new MetricRegistry();
    private static final int METRICS_REPORT_PERIOD = 10; // in seconds.

    static {
        Set<MetricAttribute> disabledMetricAttributes
                = new HashSet<>(Arrays.asList(new MetricAttribute[]{
            MetricAttribute.MAX,
            MetricAttribute.M1_RATE, MetricAttribute.M5_RATE,
            MetricAttribute.M15_RATE, MetricAttribute.MIN,
            MetricAttribute.P99, MetricAttribute.P50,
            MetricAttribute.P75, MetricAttribute.P95,
            MetricAttribute.P98, MetricAttribute.P999}));
        ConsoleReporter reporter = ConsoleReporter.forRegistry(METRICS)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MICROSECONDS)
                .disabledMetricAttributes(disabledMetricAttributes)
                .build();
        reporter.start(METRICS_REPORT_PERIOD, TimeUnit.SECONDS);
    }

    private final DataInputStream in;
    private final DataOutputStream out;

    private final LinkedBlockingQueue<Message> toWriter;
    private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
    private final Boolean batching;

    private final Writer writer;
    private final SocketReader socketReader;

    private boolean batching(Integer batchWait) {
        return batchWait > 0;
    }

    public DataRW(DataInputStream in, DataOutputStream out, Config config) {
        this.in = in;
        this.out = out;
        this.toWriter = new LinkedBlockingQueue<>();
        this.toClient = new LinkedBlockingQueue<>();
        this.batching = batching(config.getBatchWait());
        this.writer = new Writer(this.out, this.toWriter, config);
        this.socketReader = new SocketReader(this.in, this.toClient, config);
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
            doWrite(Batch.pack(message), this.out);
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

        private final Histogram batchSize;

        public Writer(DataOutputStream out, LinkedBlockingQueue<Message> toWriter, Config config) {
            this.out = out;
            this.toWriter = toWriter;
            this.batchWait = config.getBatchWait();
            this.batchSize = METRICS.histogram(MetricRegistry.name(DataRW.class, "batchSize"));
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
                        batchSize.update(ops.size());

                        MessageSet messageSet = Batch.pack(ops);
                        doWrite(messageSet, this.out);
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
                                    Metrics.startExecution(Dot.dot(reply.getCommit().getDot()));
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
        private final Deliverer deliverer;

        private ConfQueue queue;

        // metrics
        private final Timer add;
        private final Timer parse;
        private final Timer getToDeliver;
        private final Histogram queueElements;
        private final Histogram midExecution;

        private Jedis jedis = null;
        private String jedisKey = null;

        public QueueRunner(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<Reply> toQueueRunner, Config config) {

            parse = METRICS.timer(MetricRegistry.name(DataRW.class, "parse"));
            add = METRICS.timer(MetricRegistry.name(DataRW.class, "add"));
            getToDeliver = METRICS.timer(MetricRegistry.name(DataRW.class, "getToDeliver"));

            queueElements = METRICS.histogram(MetricRegistry.name(DataRW.class, "queueElements"));
            midExecution = METRICS.histogram(MetricRegistry.name(DataRW.class, "midExecution"));

            this.batching = batching(config.getBatchWait());
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
            this.jedis = new Jedis(config.getRedis());
            this.jedisKey = "" + config.getNodeNumber() + "/" + "trace-" + config.getCluster();
        }

        private void pushToRedis(Clock<ExceptionSet> committed) {
            String encoded = Trace.encode(committed);
            jedis.rpush(jedisKey, encoded);
        }

        private void pushToRedis(Dot dot, Clock<MaxInt> conf) {
            String encoded = Trace.encode(dot, conf);
            jedis.rpush(jedisKey, encoded);
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
                            queue = new ConfQueue(committed, this.batching);
                            break;

                        case COMMIT:
                            final Timer.Context parseContext = parse.time();

                            // fetch dot, dep, message and conf
                            Commit commit = reply.getCommit();
                            Dot dot = Dot.dot(commit.getDot());
                            Message message = commit.getMessage();
                            Clock<MaxInt> conf = Clock.vclock(commit.getConfMap());

                            // store trace in redis
                            if (RECORD_TRACE) {
                                pushToRedis(dot, conf);
                            }

                            midExecution.update(Metrics.midExecution(dot));
                            parseContext.stop();

                            final Timer.Context addContext = add.time();
                            Long start = System.nanoTime();
                            queue.add(dot, message, conf);
                            Long timeMicro = (System.nanoTime() - start) / 1000;
                            Metrics.endAdd(timeMicro);
                            addContext.stop();

                            final Timer.Context getToDeliverContext = getToDeliver.time();
                            List<ConfQueueBox> toDeliver = queue.getToDeliver();
                            if (!toDeliver.isEmpty()) {
                                toDeliverer.put(toDeliver);
                            }
                            queueElements.update(queue.elements());
                            getToDeliverContext.stop();
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

    private class Deliverer extends Thread {

        private final Logger LOGGER = VCDLogger.init(Deliverer.class);
        private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
        private final LinkedBlockingQueue<List<ConfQueueBox>> toDeliverer;

        private final Timer deliver;
        private final Histogram components;
        private final Histogram execution;

        public Deliverer(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<List<ConfQueueBox>> toDeliverer) {
            this.toClient = toClient;
            this.toDeliverer = toDeliverer;

            deliver = METRICS.timer(MetricRegistry.name(DataRW.class, "deliver"));
            components = METRICS.histogram(MetricRegistry.name(DataRW.class, "components"));
            execution = METRICS.histogram(MetricRegistry.name(DataRW.class, "execution"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    List<ConfQueueBox> toDeliver = toDeliverer.take();
                    components.update(toDeliver.size());

                    final Timer.Context deliverContext = deliver.time();
                    // create message set builder
                    MessageSet.Builder builder = MessageSet.newBuilder();

                    for (ConfQueueBox b : toDeliver) {
                        for (Dot d : b.getDots()) {
                            execution.update(Metrics.endExecution(d));
                        }
                        // update message set builder
                        builder.addAllMessages(b.sortMessages());
                    }
                    // build message
                    builder.setStatus(MessageSet.Status.DELIVERED);
                    MessageSet messageSet = builder.build();

                    // send it to client
                    toClient.put(Optional.of(messageSet));
                    deliverContext.stop();
                }
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }
}
