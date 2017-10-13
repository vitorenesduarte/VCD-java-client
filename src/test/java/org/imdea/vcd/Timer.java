package org.imdea.vcd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.imdea.vcd.datum.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public class Timer {

    private final List<Long> COMMITTED_TIMES;
    private final List<Long> DELIVERED_TIMES;

    public Timer() {
        this.COMMITTED_TIMES = new ArrayList<>();
        this.DELIVERED_TIMES = new ArrayList<>();
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
        sb.append("COMMITTED: ")
                .append(average(COMMITTED_TIMES))
                .append(" (us)\n");
        sb.append("DELIVERED: ")
                .append(average(DELIVERED_TIMES))
                .append(" (us)\n");
        return sb.toString();
    }

    public Map<String, String> serialize(Config config) {
        Map<String, String> m = new HashMap<>();
        m.put(
                key("VCD-commit", config),
                serialize(COMMITTED_TIMES)
        );
        m.put(
                key("VCD", config),
                serialize(DELIVERED_TIMES)
        );

        return m;
    }

    private String key(String protocol, Config config) {
        return "" + config.getNodeNumber() + "/"
                + "log-"
                + protocol + "-"
                + config.getContext() + "-"
                + config.getClients() + "-"
                + config.getConflictPercentage() + "-"
                + config.getOps() + "-"
                + "PUT";
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
        return toMicro(sum / nanos.size());
    }

    private Long toMicro(Long nano) {
        return nano / 1000;
    }

}
