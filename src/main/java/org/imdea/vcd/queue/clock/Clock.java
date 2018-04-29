package org.imdea.vcd.queue.clock;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.BiFunction;
import org.imdea.vcd.pb.Proto;
import org.imdea.vcd.pb.Proto.Dot;

/**
 *
 * @author Vitor
 * @param <T>
 */
public class Clock<T extends IntSet> {

    private final HashMap<Integer, T> map;

    public Clock(HashMap<Integer, T> map) {
        this.map = map;
    }

    public boolean contains(Dot dot) {
        return this.map.get(dot.getId()).contains(dot.getSeq());
    }

    public void merge(Clock<T> clock) {
        BiFunction<T, T, T> f = (a, b) -> {
            a.merge(b);
            return a;
        };
        for (Map.Entry<Integer, T> entry : clock.map.entrySet()) {
            this.map.merge(entry.getKey(), entry.getValue(), f);
        }
    }

    public boolean intersects(Dots dots) {
        for (Dot dot : dots) {
            if (this.contains(dot)) {
                return true;
            }
        }
        return false;
    }
    
    public static Clock<MaxInt> vclock(Map<Integer, Long> o) {
        HashMap<Integer, MaxInt> map = new HashMap<>();
        for(Map.Entry<Integer, Long> entry : o.entrySet()) {
            map.put(entry.getKey(), new MaxInt(entry.getValue()));
        }
        return new Clock<>(map);
    }
    
    public static Clock<ExceptionSet> eclock(Map<Integer, Proto.ExceptionSet> o) {
        HashMap<Integer, ExceptionSet> map = new HashMap<>();
        for(Map.Entry<Integer, Proto.ExceptionSet> entry : o.entrySet()) {
            map.put(entry.getKey(), new ExceptionSet(entry.getValue()));
        }
        return new Clock<>(map);
    }
}
