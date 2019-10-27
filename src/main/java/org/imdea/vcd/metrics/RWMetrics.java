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
    public static final Timer TO_DELIVER = METRICS.timer(MetricRegistry.name("metrics", "ToDeliver"));
    public static final Timer DELIVER_LOOP = METRICS.timer(MetricRegistry.name("metrics", "DeliverLoop"));

    private static final Timer EXECUTION0 = METRICS.timer(MetricRegistry.name("metrics", "Execution0"));
    private static final Timer EXECUTION1 = METRICS.timer(MetricRegistry.name("metrics", "Execution1"));
    private static final Timer EXECUTION2 = METRICS.timer(MetricRegistry.name("metrics", "Execution2"));

    private static final Timer COMMIT = METRICS.timer(MetricRegistry.name("metrics", "COMMIT"));
    private static final Timer DELIVER = METRICS.timer(MetricRegistry.name("metrics", "DELIVER"));

    public static final Histogram QUEUE_ELEMENTS = METRICS.histogram(MetricRegistry.name("metrics", "QueueElements"));
    public static final Histogram BATCH_SIZE = METRICS.histogram(MetricRegistry.name("metrics", "BatchSize"));
    public static final Histogram COMPONENTS_COUNT = METRICS.histogram(MetricRegistry.name("metrics", "ComponentsCount"));

    private static final ConcurrentHashMap<Dot, Timer.Context> DOT_TO_EXECUTION0_CTX = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Dot, Timer.Context> DOT_TO_EXECUTION1_CTX = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Dot, Timer.Context> DOT_TO_EXECUTION2_CTX = new ConcurrentHashMap<>();

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
        DOT_TO_EXECUTION0_CTX.put(dot, EXECUTION0.time());
        DOT_TO_EXECUTION1_CTX.put(dot, EXECUTION1.time());
        DOT_TO_EXECUTION2_CTX.put(dot, EXECUTION2.time());
    }

    public static void endExecution0(Dot dot) {
        Timer.Context context = DOT_TO_EXECUTION0_CTX.remove(dot);
        context.stop();
    }

    public static void endExecution1(Dot dot) {
        Timer.Context context = DOT_TO_EXECUTION1_CTX.remove(dot);
        context.stop();
    }

    public static void endExecution2(Dot dot) {
        Timer.Context context = DOT_TO_EXECUTION2_CTX.remove(dot);
        context.stop();
    }

    public static Timer.Context createCommitContext() {
        return COMMIT.time();
    }

    public static Timer.Context createDeliverContext() {
        return DELIVER.time();
    }
}
