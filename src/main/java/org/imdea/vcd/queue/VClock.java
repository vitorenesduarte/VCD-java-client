package org.imdea.vcd.queue;

import java.util.HashMap;
import java.util.Map;
import org.imdea.vcd.pb.Proto.Dot;

/**
 *
 * @author Vitor Enes
 */
public class VClock extends Clock {

    private final HashMap<Integer, Long> map;

    public VClock(Map<Integer, Long> map) {
        this.map = new HashMap<>(map);
    }

    @Override
    public boolean isElement(Dot dot) {
        return dot.getSeq() <= map.get(dot.getId());
    }
    
     public void merge(VClock vclock) {
        for(Map.Entry<Integer, Long> entry : vclock.map.entrySet()) {
            this.map.merge(entry.getKey(), entry.getValue(), Long::max);
        }
    }
}
