package org.imdea.vcd;

import java.util.HashMap;
import java.util.Map;
import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public class Metrics {

    private final Averager durableAvg = new Averager();
    private final Averager deliveredAvg = new Averager();
    private final Averager chainsAvg = new Averager();

    private final StringBuilder durableTimes = new StringBuilder();
    private final StringBuilder deliveredTimes = new StringBuilder();
    private final StringBuilder chains = new StringBuilder();

    public Metrics() {
    }

    public Long start() {
        return time();
    }

    public void end(MessageSet.Status status, Long start) {
        Long time = time() - start;

        switch (status) {
            case DURABLE:
                durableAvg.add(time);
                durableTimes.append(time).append("\n");
                break;
            case DELIVERED:
                deliveredAvg.add(time);
                deliveredTimes.append(time).append("\n");
                break;
        }
    }

    public void chain(Integer size) {
        chainsAvg.add(size.longValue());
        chains.append(time())
                .append("-")
                .append(size)
                .append("\n");
    }

    public String show() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("CHAINS: ")
                .append(chainsAvg.getAverage())
                .append("\n");
        sb.append("DURABLE: ")
                .append(durableAvg.getAverage())
                .append(" (ms)\n");
        sb.append("DELIVERED: ")
                .append(deliveredAvg.getAverage())
                .append(" (ms)\n");
        return sb.toString();
    }

    public Map<String, String> serialize(Config config) {
        Map<String, String> m = new HashMap<>();
        m.put(
                key(config, "chains"),
                serialize(chains)
        );
        m.put(
                key(config, "log", "Durable"),
                serialize(durableTimes)
        );
        m.put(
                key(config, "log"),
                serialize(deliveredTimes)
        );

        return m;
    }

    private String key(Config config, String prefix) {
        return key(config, prefix, "");
    }

    private String key(Config config, String prefix, String protocolSuffix) {
        return "" + config.getNodeNumber() + "/"
                + prefix + "-"
                + protocol(config.getMaxFaults(), protocolSuffix) + "-"
                + config.getCluster() + "-"
                + config.getClients() + "-"
                + config.getConflicts() + "-"
                + "100" + "-" // percentage of writes
                + config.getOp();
    }

    private String protocol(Integer maxFaults, String protocolSuffix) {
        return "VCD" + "f" + maxFaults + protocolSuffix;
    }

    private String serialize(StringBuilder sb) {
        int len = sb.length();
        if (len > 0 && sb.charAt(len - 1) == '\n') {
            sb.deleteCharAt(len - 1);
        }
        return sb.toString();
    }

    private Long time() {
        return System.currentTimeMillis();
    }

    private class Averager {
        private Long elements;
        private Long average;

        public Averager() {
            this.elements = 0L;
            this.average = 0L;
        }

        public void add(Long value) {
            this.average = (this.average * this.elements + value) / ++this.elements;
        }

        public Long getAverage() {
            return average;
        }
    }
}
