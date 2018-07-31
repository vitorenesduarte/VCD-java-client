package org.imdea.vcd;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;
import org.imdea.vcd.pb.Proto.Reply;
import org.imdea.vcd.queue.box.CommittedQueueBox;
import org.imdea.vcd.queue.DepQueue;
import org.imdea.vcd.queue.clock.Clock;

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
import org.imdea.vcd.queue.Queue;

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
    private final Integer batchWait;

    private final LinkedBlockingQueue<MessageSet> toWriter;
    private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
    private final Writer writer;
    private final SocketReader socketReader;

    public DataRW(DataInputStream in, DataOutputStream out, Config config) {
        this.in = in;
        this.out = out;
        this.batchWait = config.getBatchWait();
        this.toWriter = new LinkedBlockingQueue<>();
        this.toClient = new LinkedBlockingQueue<>();
        this.writer = new Writer(this.out, this.toWriter, this.batchWait);
        this.socketReader = new SocketReader(this.in, this.toClient, config);
    }

    public void start() {
        // start writer, if batching enabled
        if (this.batchWait > 0) {
            this.writer.start();
        }
        this.socketReader.start();
    }

    public void close() throws IOException {
        if (this.batchWait > 0) {
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

    public void write(MessageSet messageSet) throws IOException, InterruptedException {
        if (this.batchWait > 0) {
            toWriter.put(messageSet);
        } else {
            doWrite(messageSet, this.out);
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

        private final LinkedBlockingQueue<MessageSet> toWriter;
        private final DataOutputStream out;
        private final Integer batchWait;
        private final Histogram batchSize;

        public Writer(DataOutputStream out, LinkedBlockingQueue<MessageSet> toWriter, Integer batchWait) {
            this.out = out;
            this.toWriter = toWriter;
            this.batchWait = batchWait;
            this.batchSize = METRICS.histogram(MetricRegistry.name(DataRW.class, "batchSize"));
        }

        @Override
        public void run() {
            try {
                try {
                    while (true) {
                        MessageSet first = toWriter.take();
                        List<MessageSet> rest = new ArrayList<>();

                        // wait, and drain the queue
                        Thread.sleep(this.batchWait);
                        toWriter.drainTo(rest);

                        rest.add(first);

                        // create a message set with all messages in the batch
                        // also compute the set of all hashes
                        MessageSet.Builder builder = MessageSet.newBuilder();
                        HashSet<ByteString> hashes = new HashSet<>();
                        for (MessageSet ms : rest) {
                            for (Message m : ms.getMessagesList()) {
                                builder.addMessages(m);
                                hashes.addAll(m.getHashesList());
                            }
                        }
                        MessageSet allMessages = builder.build();

                        // batch size metrics
                        batchSize.update(allMessages.getMessagesCount());

                        // create a message that has as data
                        // a protobuf with the previous message set
                        Message theMessage = Message.newBuilder()
                                .addAllHashes(hashes)
                                .setData(allMessages.toByteString())
                                .build();

                        // create a message set to be pushed to the server
                        MessageSet theMessageSet = MessageSet.newBuilder()
                                .addMessages(theMessage)
                                .setStatus(MessageSet.Status.START)
                                .build();
                        doWrite(theMessageSet, this.out);
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

    // socket reader -> client | deliverer
    // deliverer -> sorter
    // sorter -> client
    private class SocketReader extends Thread {

        private final Logger LOGGER = VCDLogger.init(SocketReader.class);

        private final DataInputStream in;
        private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
        private final Integer batchWait;
        private final LinkedBlockingQueue<Reply> toDeliverer;
        private final Deliverer deliverer;

        public SocketReader(DataInputStream in, LinkedBlockingQueue<Optional<MessageSet>> toClient, Config config) {
            this.in = in;
            this.toClient = toClient;
            this.batchWait = config.getBatchWait();
            this.toDeliverer = new LinkedBlockingQueue<>();
            this.deliverer = new Deliverer(this.toClient, this.toDeliverer, config);
        }

        public void close() {
            this.deliverer.close();
            this.deliverer.interrupt();
        }

        @Override
        public void run() {
            // start deliverer
            this.deliverer.start();

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

                                if (this.batchWait > 0) {
                                    MessageSet batch = MessageSet.parseFrom(durable.getMessages(0).getData());

                                    for (Message m : batch.getMessagesList()) {
                                        MessageSet singleDurable = MessageSet.newBuilder().
                                                addMessages(m)
                                                .setStatus(MessageSet.Status.DURABLE)
                                                .build();
                                        toClient.put(Optional.of(singleDurable));
                                    }
                                } else {
                                    toClient.put(Optional.of(durable));
                                }
                                break;
                            default:
                                toDeliverer.put(reply);
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

    private class Deliverer extends Thread {

        private final Logger LOGGER = VCDLogger.init(Deliverer.class);

        private final LinkedBlockingQueue<Reply> toDeliverer;
        private final LinkedBlockingQueue<List<CommittedQueueBox>> toSorter;
        private final Sorter sorter;
        private final Boolean deliverByConf;
        private Queue<CommittedQueueBox> queue;

        // metrics
        private final Timer toAdd;
        private final Timer createBox;
        private final Timer tryDeliver;
        private final Histogram queueSize;
        private final Histogram queueElements;

        public Deliverer(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<Reply> toDeliverer, Config config) {

            createBox = METRICS.timer(MetricRegistry.name(DataRW.class, "createBox"));
            toAdd = METRICS.timer(MetricRegistry.name(DataRW.class, "toAdd"));
            tryDeliver = METRICS.timer(MetricRegistry.name(DataRW.class, "tryDeliver"));

            queueSize = METRICS.histogram(MetricRegistry.name(DataRW.class, "queueSize"));
            queueElements = METRICS.histogram(MetricRegistry.name(DataRW.class, "queueElements"));

            this.toDeliverer = toDeliverer;
            this.toSorter = new LinkedBlockingQueue<>();
            this.sorter = new Sorter(toClient, this.toSorter, config);
            this.deliverByConf = config.getDeliverByConf();
        }

        public void close() {
            this.sorter.interrupt();
        }

        @Override
        public void run() {
            // start sorter
            this.sorter.start();

            try {
                while (true) {
                    Reply reply = toDeliverer.take();

                    switch (reply.getReplyCase()) {
                        case INIT:
                            if (this.deliverByConf) {
                                this.queue = new ConfQueue(Clock.eclock(reply.getInit().getCommittedMap()));
                            } else {
                                this.queue = new DepQueue(Clock.eclock(reply.getInit().getCommittedMap()));
                            }
                            break;
                        case COMMIT:
                            final Timer.Context createBoxContext = createBox.time();
                            CommittedQueueBox box = new CommittedQueueBox(reply.getCommit());
                            createBoxContext.stop();

                            final Timer.Context toAddContext = toAdd.time();
                            queue.add(box);
                            toAddContext.stop();

                            final Timer.Context tryDeliverContext = tryDeliver.time();
                            List<CommittedQueueBox> toDeliver = queue.tryDeliver();
                            tryDeliverContext.stop();

                            queueSize.update(queue.size());
                            queueElements.update(queue.elements());

                            if (!toDeliver.isEmpty()) {
                                toSorter.put(toDeliver);
                            }
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

    private class Sorter extends Thread {

        private final Logger LOGGER = VCDLogger.init(Sorter.class);
        private final LinkedBlockingQueue<Optional<MessageSet>> toClient;
        private final LinkedBlockingQueue<List<CommittedQueueBox>> toSorter;
        private final Integer batchWait;

        private final Timer sorting;
        private final Histogram toSort;

        public Sorter(LinkedBlockingQueue<Optional<MessageSet>> toClient, LinkedBlockingQueue<List<CommittedQueueBox>> toSorter, Config config) {
            this.toClient = toClient;
            this.toSorter = toSorter;
            this.batchWait = config.getBatchWait();

            this.sorting = METRICS.timer(MetricRegistry.name(DataRW.class, "sorting"));
            this.toSort = METRICS.histogram(MetricRegistry.name(DataRW.class, "toSort"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    List<CommittedQueueBox> toDeliver = toSorter.take();
                    toSort.update(toDeliver.size());

                    final Timer.Context sortingContext = sorting.time();
                    MessageSet.Builder builder = MessageSet.newBuilder();
                    for (CommittedQueueBox boxToDeliver : toDeliver) {
                        for (Message message : boxToDeliver.sortMessages()) {
                            if (this.batchWait > 0) {
                                MessageSet batch = MessageSet.parseFrom(message.getData());
                                builder.addAllMessages(batch.getMessagesList());
                            } else {
                                builder.addMessages(message);
                            }
                        }
                    }
                    builder.setStatus(MessageSet.Status.DELIVERED);
                    MessageSet messageSet = builder.build();
                    sortingContext.stop();

                    toClient.put(Optional.of(messageSet));
                }
            } catch (InterruptedException | IOException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }
}
