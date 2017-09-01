package org.imdea.vcd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author Vitor Enes
 */
public class Debug {

    private static final Map<String, Long> START = new TreeMap<>();
    private static final Map<String, List<Long>> TIMES = new TreeMap<>();

    public static void start(String method) {
        Long start = System.nanoTime();
        START.put(method, start);
    }

    public static void end(String method) {
        Long time = System.nanoTime() - START.get(method);

        List<Long> times = TIMES.get(method);
        if (times == null) {
            times = new ArrayList<>();
        }
        times.add(time);

        TIMES.put(method, times);
    }

    public static void show() {
        for(String method : TIMES.keySet()){
            System.out.println(method + ": " + average(TIMES.get(method)) + " (us)");
        }
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
