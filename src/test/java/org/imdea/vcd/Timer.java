package org.imdea.vcd;

import java.util.ArrayList;
import java.util.List;
import org.imdea.vcd.datum.Status;

/**
 *
 * @author Vitor Enes
 */
public class Timer {

    private static final List<Long> WRITTEN_TIMES = new ArrayList<>();
    private static final List<Long> COMMITTED_TIMES = new ArrayList<>();
    private static final List<Long> DELIVERED_TIMES = new ArrayList<>();

    public static Long start() {
        return System.nanoTime();
    }

    public static void end(Status status, Long start) {
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

    public static void show() {
        assert WRITTEN_TIMES.size() == COMMITTED_TIMES.size()
                && COMMITTED_TIMES.size() == DELIVERED_TIMES.size();
        System.out.println(Status.WRITTEN + ": " + average(WRITTEN_TIMES) + " (us)");
        System.out.println(Status.COMMITTED + ": " + average(COMMITTED_TIMES) + " (us)");
        System.out.println(Status.DELIVERED + ": " + average(DELIVERED_TIMES) + " (us)");
    }

    public static String serialize() {
        StringBuilder sb = new StringBuilder();
        append(sb, "written", WRITTEN_TIMES);
        append(sb, "committed", COMMITTED_TIMES);
        append(sb, "delivered", DELIVERED_TIMES);
        sb.append("|");
        return sb.toString();
    }

    private static void append(StringBuilder sb, String key, List<Long> times) {
        sb.append(key).append("=");
        for (Long time : times) {
            sb.append(time).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(";");
    }

    private static Long average(List<Long> nanos) {
        Long sum = 0L;
        for (Long nano : nanos) {
            sum += nano;
        }
        return toMicro(sum / nanos.size());
    }

    private static Long toMicro(Long nano) {
        return nano / 1000;
    }

}
