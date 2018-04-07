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

    private final List<ChainMetric> CHAIN_LENGTHS;
    private final List<Long> COMMITTED_TIMES;
    private final List<Long> DELIVERED_TIMES;

    public Metrics() {
        this.CHAIN_LENGTHS = new LinkedList<>();
        this.COMMITTED_TIMES = new LinkedList<>();
        this.DELIVERED_TIMES = new LinkedList<>();
    }

    public void chain(Integer size) {
        CHAIN_LENGTHS.add(new ChainMetric(time(), new Long(size)));
    }

    public Long start() {
        return time();
    }

    public void end(MessageSet.Status status, Long start) {
        Long time = time() - start;

        switch (status) {
            case COMMITTED:
                COMMITTED_TIMES.add(time);
                break;
            case DELIVERED:
                DELIVERED_TIMES.add(time);
                break;
        }
    }

    public String show() {
        assert COMMITTED_TIMES.isEmpty() || COMMITTED_TIMES.size() == DELIVERED_TIMES.size();
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("CHAINS: ")
                .append(averageChains(CHAIN_LENGTHS))
                .append("\n");
        sb.append("COMMITTED: ")
                .append(toMs(average(COMMITTED_TIMES)))
                .append(" (ms)\n");
        sb.append("DELIVERED: ")
                .append(toMs(average(DELIVERED_TIMES)))
                .append(" (ms)\n");
        return sb.toString();
    }

    public Map<String, String> serialize(Config config) {
        Map<String, String> m = new HashMap<>();
        m.put(
                key(config, "Chains"),
                serializeChains(CHAIN_LENGTHS)
        );
        m.put(
                key(config, "Commit"),
                serializeTimes(COMMITTED_TIMES)
        );
        m.put(
                key(config),
                serializeTimes(DELIVERED_TIMES)
        );

        return m;
    }

    private String key(Config config) {
        return key(config, "");
    }

    private String key(Config config, String protocolSuffix) {
        return "" + config.getNodeNumber() + "/"
                + "log-"
                + protocol(config.getMaxFaults(), protocolSuffix) + "-"
                + config.getCluster() + "-"
                + config.getClients() + "-"
                + conflictPercentage(config.getConflicts()) + "-"
                + config.getOps() + "-"
                + config.getOp();
    }

    private String protocol(Integer maxFaults, String protocolSuffix) {
        return "VCD" + "f" + maxFaults + protocolSuffix;
    }

    private String conflictPercentage(boolean conflicts) {
        if (conflicts) {
            return "100";
        } else {
            return "0";
        }
    }

    private String serializeTimes(List<Long> metrics) {
        StringBuilder sb = new StringBuilder();
        for (Long metric : metrics) {
            sb.append(metric).append(",");
        }
        if (metrics.size() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(";");
        return sb.toString();
    }

    private String serializeChains(List<ChainMetric> metrics) {
        StringBuilder sb = new StringBuilder();
        for (ChainMetric metric : metrics) {
            sb.append(metric.getTime())
                    .append("-")
                    .append(metric.getSize())
                    .append(",");
        }
        if (metrics.size() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(";");
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
        return System.nanoTime();
    }

    private Long toMs(Long nano) {
        return nano / 1000000;
    }

}
