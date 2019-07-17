package org.imdea.vcd.metrics;

import org.imdea.vcd.Config;
import org.imdea.vcd.pb.Proto.MessageSet;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Vitor Enes
 */
public class ClientMetrics {

    private static final Averager COMMIT_AVG = new Averager();
    private static final Averager DELIVERED_AVG = new Averager();
    private static final Averager CHAINS_AVG = new Averager();

    private static final StringBuilder COMMIT_TIMES = new StringBuilder();
    private static final StringBuilder DELIVERED_TIMES = new StringBuilder();
    private static final StringBuilder TIMESERIES = new StringBuilder();
    private static final StringBuilder CHAINS = new StringBuilder();
    private static final StringBuilder QUEUE = new StringBuilder();

    public static Long start() {
        return time();
    }

    public static void end(MessageSet.Status status, Long start) {
        Long time = time() - start;

        switch (status) {
            case COMMIT:
                COMMIT_AVG.add(time);
                COMMIT_TIMES.append(time).append("\n");
                break;
            case DELIVERED:
                DELIVERED_AVG.add(time);
                DELIVERED_TIMES.append(time).append("\n");
                TIMESERIES.append(time()).append("-").append("1").append("\n");
                break;
        }
    }

    public static void chain(Integer size) {
        CHAINS.append(size).append("\n");
        CHAINS_AVG.add(size.longValue());
    }

    public static void queue(String r) {
        synchronized (QUEUE) {
            QUEUE.append(r).append("\n");
        }
    }

    public static String show() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("CHAINS: ")
                .append(CHAINS_AVG.getAverage())
                .append("\n");
        sb.append("COMMIT: ")
                .append(COMMIT_AVG.getAverage())
                .append(" (ms)\n");
        sb.append("DELIVERED: ")
                .append(DELIVERED_AVG.getAverage())
                .append(" (ms)\n");
        return sb.toString();
    }

    public static Map<String, String> serialize(Config config) {
        Map<String, String> m = new HashMap<>();
        m.put(
                key(config, "chains"),
                serialize(CHAINS)
        );
        m.put(
                key(config, "log", "Commit"),
                serialize(COMMIT_TIMES)
        );
        m.put(
                key(config, "log"),
                serialize(DELIVERED_TIMES)
        );
//        m.put(
//                key(config, "queue"),
//                serialize(QUEUE)
//        );
        m.put(
                key(config, "timeseries"),
                serialize(TIMESERIES)
        );

        return m;
    }

    private static String key(Config config, String prefix) {
        return key(config, prefix, "");
    }

    private static String key(Config config, String prefix, String protocolSuffix) {
        return "" + prefix + "-"
                + protocol(config, protocolSuffix) + "-"
                + config.getCluster() + "-"
                + config.getClients() + "-"
                + config.getConflicts() + "-"
                + "100" + "-" // percentage of writes
                + config.getOp();
    }

    private static String protocol(Config config, String protocolSuffix) {
        protocolSuffix += config.getBatching() ? "Batching" : "";
        protocolSuffix += !config.getOptDelivery() ? "NonOptDelivery" : "";
        protocolSuffix += !config.getClosedLoop() ? "OpenLoop" + config.getSleep(): "";
        return protocolName(config) + protocolSuffix;
    }

    private static String protocolName(Config config) {
        if (config.getProtocol().equals("vcd")) {
            return "vcd" + "f" + config.getMaxFaults();
        } else {
            return config.getProtocol();
        }
    }

    private static String serialize(StringBuilder sb) {
        int len = sb.length();
        if (len > 0 && sb.charAt(len - 1) == '\n') {
            sb.deleteCharAt(len - 1);
        }
        return sb.toString();
    }

    private static Long time() {
        return System.currentTimeMillis();
    }

    private static class Averager {

        private Float elements;
        private Float average;

        public Averager() {
            this.elements = 0F;
            this.average = 0F;
        }

        public void add(Long value) {
            this.average = (this.average * this.elements + value) / ++this.elements;
        }

        public Float getAverage() {
            return average;
        }
    }
}
