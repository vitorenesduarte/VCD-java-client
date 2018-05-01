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
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Vitor Enes
 */
public class DataRW {

    private static final int METRICS_REPORT_PERIOD=10; // in seconds.
    private final MetricRegistry metrics;
    private final Timer toAdd;
    private final Timer createBox;
    private final Timer tryDeliver;
    private final Timer sorting;

    private final DataInputStream in;
    private final DataOutputStream out;
    private DependencyQueue<CommitDepBox> queue;

    public DataRW(DataInputStream in, DataOutputStream out, Integer nodeNumber) {
        this.in = in;
        this.out = out;

        metrics = new MetricRegistry();
        Set<MetricAttribute> disabledMetricAttributes  =
                new HashSet<>(Arrays.asList(new MetricAttribute[]{
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

        createBox = metrics.timer(MetricRegistry.name(DataRW.class,"createBox"));
        toAdd = metrics.timer(MetricRegistry.name(DataRW.class,"toAdd"));
        tryDeliver = metrics.timer(MetricRegistry.name(DataRW.class,"tryDeliver"));
        sorting = metrics.timer(MetricRegistry.name(DataRW.class,"sorting"));
    }

    public void write(MessageSet messageSet) throws IOException {
        byte[] data = messageSet.toByteArray();
        out.writeInt(data.length);
        out.write(data, 0, data.length);
        out.flush();
    }

    public MessageSet read() throws IOException {
        MessageSet result = null;
        while (result == null) {
            result = doRead();
        }
        return result;
    }

    private MessageSet doRead() throws IOException {
        int length = in.readInt();
        byte data[] = new byte[length];
        in.readFully(data, 0, length);

        long start;

        // check if reply is a message set
        // if yes, return it,
        // otherwise feed the dependency queue
        // with the commit notification
        Reply reply = Reply.parseFrom(data);
        switch (reply.getReplyCase()) {
            case INIT:
                this.queue = new DependencyQueue(Clock.eclock(reply.getInit().getCommittedMap()));
                return null;
            case SET:
                return reply.getSet();
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

                if (toDeliver.isEmpty()) {
                    return null;
                } else {
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
                    return messageSet;
                }
            default:
                throw new RuntimeException("Reply type not supported:" + reply.getReplyCase());
        }
    }

    public void close() throws IOException {
        this.in.close();
        this.out.close();
    }
}
