package org.imdea.vcd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public class Metrics {

    private final List<Long> CHAIN_LENGTHS;
    private final List<Long> COMMITTED_TIMES;
    private final List<Long> DELIVERED_TIMES;

    public Metrics() {
        this.CHAIN_LENGTHS = new ArrayList<>();
        this.COMMITTED_TIMES = new ArrayList<>();
        this.DELIVERED_TIMES = new ArrayList<>();
    }

    public void chain(Integer size) {
        chain(new Long(size));
    }

    public void chain(Long size) {
        CHAIN_LENGTHS.add(size);
    }

    public Long start() {
        return System.nanoTime();
    }

    public void end(MessageSet.Status status, Long start) {
        Long time = System.nanoTime() - start;

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
        sb.append("CHAINS: ")
                .append(average(CHAIN_LENGTHS))
                .append("\n");
        sb.append("COMMITTED: ")
                .append(toMs(average(COMMITTED_TIMES)))
                .append(" (us)\n");
        sb.append("DELIVERED: ")
                .append(toMs(average(DELIVERED_TIMES)))
                .append(" (us)\n");
        return sb.toString();
    }

    public Map<String, String> serialize(Config config) {
        Map<String, String> m = new HashMap<>();
        m.put(
                key(config, "Chains"),
                serialize(CHAIN_LENGTHS)
        );
        m.put(
                key(config, "Commit"),
                serialize(COMMITTED_TIMES)
        );
        m.put(
                key(config),
                serialize(DELIVERED_TIMES)
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

    private String serialize(List<Long> times) {
        StringBuilder sb = new StringBuilder();
        for (Long time : times) {
            sb.append(time).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(";");
        return sb.toString();
    }

    private Long average(List<Long> nanos) {
        int size = nanos.size();
        if (size == 0) {
            return 0L;
        }
        Long sum = 0L;
        for (Long nano : nanos) {
            sum += nano;
        }
        return sum / nanos.size();
    }

    private Long toMs(Long nano) {
        return nano / 1000000;
    }

}
