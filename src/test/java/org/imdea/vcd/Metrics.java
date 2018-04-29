package org.imdea.vcd;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public class Metrics {

    private class ChainMetric {

        private final Long time;
        private final Long size;

        public ChainMetric(Long time, Long size) {
            this.time = time;
            this.size = size;
        }

        public Long getTime() {
            return time;
        }

        public Long getSize() {
            return size;
        }
    }

    private final List<ChainMetric> chainLengths;
    private final List<Long> durableTimes;
    private final List<Long> deliveredTimes;

    public Metrics() {
        this.chainLengths = new LinkedList<>();
        this.durableTimes = new LinkedList<>();
        this.deliveredTimes = new LinkedList<>();
    }

    public void chain(Integer size) {
        chainLengths.add(new ChainMetric(time(), new Long(size)));
    }

    public Long start() {
        return time();
    }

    public void end(MessageSet.Status status, Long start) {
        Long time = time() - start;

        switch (status) {
            case DURABLE:
                durableTimes.add(time);
                break;
            case DELIVERED:
                deliveredTimes.add(time);
                break;
        }
    }

    public String show() {
        assert durableTimes.isEmpty() || durableTimes.size() == deliveredTimes.size();
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("CHAINS: ")
                .append(averageChains(chainLengths))
                .append("\n");
        sb.append("DURABLE: ")
                .append(average(durableTimes))
                .append(" (ms)\n");
        sb.append("DELIVERED: ")
                .append(average(deliveredTimes))
                .append(" (ms)\n");
        return sb.toString();
    }

    public Map<String, String> serialize(Config config) {
        Map<String, String> m = new HashMap<>();
        m.put(
                key(config, "chains"),
                serializeChains(chainLengths)
        );
        m.put(
                key(config, "log", "Durable"),
                serializeTimes(durableTimes)
        );
        m.put(
                key(config, "log"),
                serializeTimes(deliveredTimes)
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

    private String serializeTimes(List<Long> metrics) {
        StringBuilder sb = new StringBuilder();
        for (Long metric : metrics) {
            sb.append(metric).append("\n");
        }
        if (metrics.size() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    private String serializeChains(List<ChainMetric> metrics) {
        StringBuilder sb = new StringBuilder();
        for (ChainMetric metric : metrics) {
            sb.append(metric.getTime())
                    .append("-")
                    .append(metric.getSize())
                    .append("\n");
        }
        if (metrics.size() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    private Long average(List<Long> list) {
        int size = list.size();
        if (size == 0) {
            return 0L;
        }
        Long sum = 0L;
        for (Long elem : list) {
            sum += elem;
        }
        return sum / list.size();
    }

    private Long averageChains(List<ChainMetric> metrics) {
        List<Long> sizes = new LinkedList<>();
        for (ChainMetric metric : metrics) {
            sizes.add(metric.getSize());
        }
        return average(sizes);
    }

    private Long time() {
        return System.currentTimeMillis();
    }
}
