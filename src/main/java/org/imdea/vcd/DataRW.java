package org.imdea.vcd;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;
import org.imdea.vcd.pb.Proto.Reply;
import org.imdea.vcd.queue.CommitDepBox;
import org.imdea.vcd.queue.DependencyQueue;
import org.imdea.vcd.queue.clock.Clock;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Vitor Enes
 */
public class DataRW {

    private final DataInputStream in;
    private final DataOutputStream out;
    private final LinkedBlockingQueue<MessageSet> toClient;

    public DataRW(DataInputStream in, DataOutputStream out, Integer nodeNumber) {
        this.in = in;
        this.out = out;
        this.toClient = new LinkedBlockingQueue<>();
    }

    public void start() {
        SocketReader reader = new SocketReader(this.in, this.toClient);
        reader.start();
    }

    public void write(MessageSet messageSet) throws IOException {
        byte[] data = messageSet.toByteArray();
        this.out.writeInt(data.length);
        this.out.write(data, 0, data.length);
        this.out.flush();
    }

    public MessageSet read() throws IOException, InterruptedException {
        return this.toClient.take();
    }

    // socket reader -> client | deliverer
    // deliverer -> client
    private class SocketReader extends Thread {

        private final Logger LOGGER = VCDLogger.init(SocketReader.class);

        private final DataInputStream in;
        private final LinkedBlockingQueue<MessageSet> toClient;
        private final LinkedBlockingQueue<Reply> toDeliverer;

        public SocketReader(DataInputStream in, LinkedBlockingQueue<MessageSet> toClient) {
            this.in = in;
            this.toClient = toClient;
            this.toDeliverer = new LinkedBlockingQueue<>();
        }

        @Override
        public void run() {
            // start deliverer
            Deliverer deliverer = new Deliverer(this.toClient, this.toDeliverer);
            deliverer.start();

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
                            toClient.put(reply.getSet());
                            break;
                        default:
                            toDeliverer.put(reply);
                            break;
                    }
                }
            } catch (IOException | InterruptedException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }
    }

    private class Deliverer extends Thread {

        private final Logger LOGGER = VCDLogger.init(Deliverer.class);

        private final LinkedBlockingQueue<MessageSet> toClient;
        private final LinkedBlockingQueue<Reply> toDeliverer;
        private DependencyQueue<CommitDepBox> queue;

        // metrics
        private final int METRICS_REPORT_PERIOD = 10; // in seconds.
        private final MetricRegistry metrics;
        private final Timer toAdd;
        private final Timer createBox;
        private final Timer tryDeliver;
        private final Timer sorting;

        public Deliverer(LinkedBlockingQueue<MessageSet> toClient, LinkedBlockingQueue<Reply> toDeliverer) {
            this.toClient = toClient;
            this.toDeliverer = toDeliverer;

            metrics = new MetricRegistry();
            Set<MetricAttribute> disabledMetricAttributes
                    = new HashSet<>(Arrays.asList(new MetricAttribute[]{
                MetricAttribute.MAX, MetricAttribute.STDDEV,
                MetricAttribute.M1_RATE, MetricAttribute.M5_RATE,
                MetricAttribute.M15_RATE, MetricAttribute.MIN,
                MetricAttribute.P99, MetricAttribute.P50,
                MetricAttribute.P75, MetricAttribute.P95,
                MetricAttribute.P98, MetricAttribute.P999}));
            ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MICROSECONDS)
                    .disabledMetricAttributes(disabledMetricAttributes)
                    .build();
            reporter.start(METRICS_REPORT_PERIOD, TimeUnit.SECONDS);

            createBox = metrics.timer(MetricRegistry.name(DataRW.class, "createBox"));
            toAdd = metrics.timer(MetricRegistry.name(DataRW.class, "toAdd"));
            tryDeliver = metrics.timer(MetricRegistry.name(DataRW.class, "tryDeliver"));
            sorting = metrics.timer(MetricRegistry.name(DataRW.class, "sorting"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Reply reply = toDeliverer.take();

                    switch (reply.getReplyCase()) {
                        case INIT:
                            this.queue = new DependencyQueue(Clock.eclock(reply.getInit().getCommittedMap()));
                            break;
                        case COMMIT:
                            final Timer.Context createBoxContext = toAdd.time();
                            CommitDepBox box = new CommitDepBox(reply.getCommit());
                            createBoxContext.stop();

                            final Timer.Context toAddContext = createBox.time();
                            queue.add(box);
                            toAddContext.stop();

                            final Timer.Context tryDeliverContext = tryDeliver.time();
                            List<CommitDepBox> toDeliver = queue.tryDeliver();
                            tryDeliverContext.stop();

                            if (!toDeliver.isEmpty()) {
                                final Timer.Context sortingContext = sorting.time();
                                MessageSet.Builder builder = MessageSet.newBuilder();
                                for (CommitDepBox boxToDeliver : toDeliver) {
                                    for (Message message : boxToDeliver.sortMessages()) {
                                        builder.addMessages(message);
                                    }
                                }
                                builder.setStatus(MessageSet.Status.DELIVERED);
                                MessageSet messageSet = builder.build();
                                sortingContext.stop();

                                toClient.put(messageSet);
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

    public void close() throws IOException {
        this.in.close();
        this.out.close();
    }
}
