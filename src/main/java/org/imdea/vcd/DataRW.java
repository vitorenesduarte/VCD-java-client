package org.imdea.vcd;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
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

    private final LinkedBlockingQueue<MessageSet> toWriter;
    private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
    private final WriteDelay writeDelay;

    private final Writer writer;
    private final SocketReader socketReader;

    public DataRW(DataInputStream in, DataOutputStream out, Config config) {
        this.in = in;
        this.out = out;
        this.toWriter = new LinkedBlockingQueue<>();
        this.toClient = new LinkedBlockingQueue<>();
        this.writeDelay = new WriteDelay(config.getWriteDelay());
        this.writer = new Writer(this.out, this.toWriter, this.writeDelay);
        this.socketReader = new SocketReader(this.in, this.toClient, this.writeDelay, config);
    }

    public void start() {
        this.writer.start();
        this.socketReader.start();
    }

    public void close() throws IOException {
        this.writer.interrupt();
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

    public void write(MessageSet messageSet) throws IOException, InterruptedException {
        toWriter.put(messageSet);
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

        private final LinkedBlockingQueue<MessageSet> toWriter;
        private final DataOutputStream out;
        private final Histogram batchSize;
        private final Timer delayWrite;
        private final WriteDelay writeDelay;

        public Writer(DataOutputStream out, LinkedBlockingQueue<MessageSet> toWriter, WriteDelay writeDelay) {
            this.out = out;
            this.toWriter = toWriter;
            this.writeDelay = writeDelay;
            this.batchSize = METRICS.histogram(MetricRegistry.name(DataRW.class, "batchSize"));
            this.delayWrite = METRICS.timer(MetricRegistry.name(DataRW.class, "delayWrite"));
        }

        @Override
        public void run() {
            try {
                try {
                    while (true) {
                        MessageSet first = toWriter.take();
                        List<MessageSet> set = new ArrayList<>();

                        // wait, and drain the queue
                        final Timer.Context delayWriteTimer = delayWrite.time();
                        writeDelay.waitDepsCommitted();
                        delayWriteTimer.stop();

                        toWriter.drainTo(set);

                        set.add(first);

                        // batch size metrics
                        batchSize.update(set.size());

                        for (MessageSet m : set) {
                            doWrite(m, this.out);
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
        private final QueueRunner queueRunner;

        public SocketReader(DataInputStream in, LinkedBlockingQueue<Optional<MessageSet>> toClient, WriteDelay writeDelay, Config config) {
            this.in = in;
            this.toClient = toClient;
            this.toQueueRunner = new LinkedBlockingQueue<>();
            this.queueRunner = new QueueRunner(this.toClient, this.toQueueRunner, writeDelay, config);
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

                                toClient.put(Optional.of(durable));
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

        private final Logger LOGGER = VCDLogger.init(QueueRunner.class);

        private final LinkedBlockingQueue<Reply> toQueueRunner;
        private final LinkedBlockingQueue<List<ConfQueueBox>> toDeliverer;
        private final WriteDelay writeDelay;
        private final Deliverer deliverer;

        private ConfQueue queue;

        // metrics
        private final Timer add;
        private final Timer parse;
        private final Timer getToDeliver;
        private final Histogram queueElements;
        private final Histogram midExecution;

        public QueueRunner(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<Reply> toQueueRunner, WriteDelay writeDelay, Config config) {

            parse = METRICS.timer(MetricRegistry.name(DataRW.class, "parse"));
            add = METRICS.timer(MetricRegistry.name(DataRW.class, "add"));
            getToDeliver = METRICS.timer(MetricRegistry.name(DataRW.class, "getToDeliver"));

            queueElements = METRICS.histogram(MetricRegistry.name(DataRW.class, "queueElements"));
            midExecution = METRICS.histogram(MetricRegistry.name(DataRW.class, "midExecution"));

            this.toQueueRunner = toQueueRunner;
            this.writeDelay = writeDelay;
            this.toDeliverer = new LinkedBlockingQueue<>();
            this.deliverer = new Deliverer(toClient, this.toDeliverer, this.writeDelay);
        }

        public void close() {
            this.deliverer.interrupt();
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
                            this.writeDelay.init(reply.getInit());

                            // create delivery queue
                            this.queue = new ConfQueue(committed);
                            break;

                        case COMMIT:
                            final Timer.Context parseContext = parse.time();

                            // fetch dot, dep, message and conf
                            Commit commit = reply.getCommit();
                            Dot dot = Dot.dot(commit.getDot());
                            Message message = commit.getMessage();
                            Clock<MaxInt> conf = Clock.vclock(commit.getConfMap());

                            // update write delay
                            writeDelay.commit(dot, conf);
                            midExecution.update(Metrics.midExecution(dot));
                            parseContext.stop();

                            final Timer.Context addContext = add.time();
                            queue.add(dot, message, conf);
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
        private final WriteDelay writeDelay;

        private final Timer deliver;
        private final Histogram components;
        private final Histogram execution;

        public Deliverer(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<List<ConfQueueBox>> toDeliverer, WriteDelay writeDelay) {
            this.toClient = toClient;
            this.toDeliverer = toDeliverer;
            this.writeDelay = writeDelay;

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

                    if (!toDeliver.isEmpty()) {
                        for (ConfQueueBox b : toDeliver) {
                            for (Dot d : b.getDots()) {
                                // update write delay
                                this.writeDelay.deliver(d);
                                execution.update(Metrics.endExecution(d));
                            }
                            // update message set builder
                            builder.addAllMessages(b.sortMessages());
                        }
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
