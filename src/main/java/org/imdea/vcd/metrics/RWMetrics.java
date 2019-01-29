package org.imdea.vcd.metrics;

import com.codahale.metrics.*;
import org.imdea.vcd.queue.clock.Dot;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class RWMetrics {

    private static final MetricRegistry METRICS = new MetricRegistry();
    private static final int METRICS_REPORT_PERIOD = 10; // in seconds.

    // metrics
    public static final Timer PARSE = METRICS.timer(MetricRegistry.name("metrics", "Parse"));
    public static final Timer QUEUE_ADD = METRICS.timer(MetricRegistry.name("metrics", "QueueAdd"));
    public static final Timer DELIVER_LOOP = METRICS.timer(MetricRegistry.name("metrics", "DeliverLoop"));

    private static final Timer MID_EXECUTION = METRICS.timer(MetricRegistry.name("metrics", "MidExecution"));
    private static final Timer EXECUTION = METRICS.timer(MetricRegistry.name("metrics", "Execution"));
    private static final Timer DURABLE = METRICS.timer(MetricRegistry.name("metrics", "DURABLE"));
    private static final Timer DELIVER = METRICS.timer(MetricRegistry.name("metrics", "DELIVER"));

    public static final Histogram QUEUE_ELEMENTS = METRICS.histogram(MetricRegistry.name("metrics", "QueueElements"));
    public static final Histogram BATCH_SIZE = METRICS.histogram(MetricRegistry.name("metrics", "BatchSize"));
    public static final Histogram COMPONENTS_COUNT = METRICS.histogram(MetricRegistry.name("metrics", "ComponentsCount"));

    private static final ConcurrentHashMap<Dot, Timer.Context> DOT_TO_MID_EXECUTION_CTX = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Dot, Timer.Context> DOT_TO_EXECUTION_CTX = new ConcurrentHashMap<>();

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

    public static void startExecution(Dot dot) {
        DOT_TO_MID_EXECUTION_CTX.put(dot, MID_EXECUTION.time());
        DOT_TO_EXECUTION_CTX.put(dot, EXECUTION.time());
    }

    public static void endMidExecution(Dot dot) {
        Timer.Context context = DOT_TO_MID_EXECUTION_CTX.remove(dot);
        context.stop();
    }

    public static void endExecution(Dot dot) {
        Timer.Context context = DOT_TO_EXECUTION_CTX.remove(dot);
        context.stop();
    }

    public static Timer.Context createDurableContext() {
        return DURABLE.time();
    }

    public static Timer.Context createDeliverContext() {
        return DELIVER.time();
    }
}
