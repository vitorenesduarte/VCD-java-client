package org.imdea.vcd.queue;

import java.util.HashMap;
import java.util.Map;
import org.imdea.vcd.pb.Proto;
import org.imdea.vcd.pb.Proto.Dot;

/**
 *
 * @author Vitor Enes
 */
public class EClock extends Clock {

    private final HashMap<Integer, ExceptionSet> map;

    public EClock(Map<Integer, Proto.ExceptionSet> map) {
        this.map = new HashMap<>();
        for(Map.Entry<Integer, Proto.ExceptionSet> entry : map.entrySet()) {
            ExceptionSet ex = new ExceptionSet(entry.getValue());
            this.map.put(entry.getKey(), ex);
        }
    }
    
    @Override
    public boolean isElement(Dot dot) {
        ExceptionSet exSet = map.get(dot.getId());
        return exSet.isElement(dot.getSeq());
    }

    public void merge(EClock eclock) {
        for(Map.Entry<Integer, ExceptionSet> entry : eclock.map.entrySet()) {
            this.map.merge(entry.getKey(), entry.getValue(), ExceptionSet::merge);
        }
    }
}