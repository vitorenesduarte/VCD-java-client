package org.imdea.vcd;

import java.util.ArrayList;
import java.util.List;
import org.imdea.vcd.datum.Status;

/**
 *
 * @author Vitor Enes
 */
public class Timer {

    private final List<Long> WRITTEN_TIMES;
    private final List<Long> COMMITTED_TIMES;
    private final List<Long> DELIVERED_TIMES;

    public Timer() {
        this.WRITTEN_TIMES = new ArrayList<>();
        this.COMMITTED_TIMES = new ArrayList<>();
        this.DELIVERED_TIMES = new ArrayList<>();
    }

    public Long start() {
        return System.nanoTime();
    }

    public void end(Status status, Long start) {
        Long time = System.nanoTime() - start;

        switch (status) {
            case WRITTEN:
                WRITTEN_TIMES.add(time);
                break;
            case COMMITTED:
                if (WRITTEN_TIMES.size() == COMMITTED_TIMES.size()) {
                    WRITTEN_TIMES.add(time);
                }
                COMMITTED_TIMES.add(time);
                break;
            case DELIVERED:
                if (WRITTEN_TIMES.size() == DELIVERED_TIMES.size()) {
                    WRITTEN_TIMES.add(time);
                }
                if (COMMITTED_TIMES.size() == DELIVERED_TIMES.size()) {
                    COMMITTED_TIMES.add(time);
                }
                DELIVERED_TIMES.add(time);
                break;
        }
    }

    public String show() {
        assert WRITTEN_TIMES.size() == COMMITTED_TIMES.size()
                && COMMITTED_TIMES.size() == DELIVERED_TIMES.size();
        StringBuilder sb = new StringBuilder();
        sb.append(Status.WRITTEN)
                .append(": ")
                .append(average(WRITTEN_TIMES))
                .append(" (us)\n");
        sb.append(Status.COMMITTED)
                .append(": ")
                .append(average(COMMITTED_TIMES))
                .append(" (us)\n");
        sb.append(Status.DELIVERED)
                .append(": ")
                .append(average(DELIVERED_TIMES))
                .append(" (us)\n");
        return sb.toString();
    }

    public String serialize() {
        StringBuilder sb = new StringBuilder();
        append(sb, "written", WRITTEN_TIMES);
        append(sb, "committed", COMMITTED_TIMES);
        append(sb, "delivered", DELIVERED_TIMES);
        sb.append("|");
        return sb.toString();
    }

    private void append(StringBuilder sb, String key, List<Long> times) {
        sb.append(key).append("=");
        for (Long time : times) {
            sb.append(time).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(";");
    }

    private Long average(List<Long> nanos) {
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
