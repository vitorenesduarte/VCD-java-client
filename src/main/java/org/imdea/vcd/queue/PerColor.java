package org.imdea.vcd.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Dot;

/**
 *
 * @author Vitor Enes
 */
public class PerColor {

    private final TreeMap<Dot, Message> map;

    public PerColor(Dot dot, Message message) {
        this.map = new TreeMap<>();
        this.map.put(dot, message);
    }

    public PerColor(PerColor perColor) {
        this.map = new TreeMap<>(perColor.map);
    }

    public void merge(PerColor o) {
        this.map.putAll(o.map);
    }

    public List<Message> sortMessages() {
        List<Message> result = new ArrayList<>(map.values());
        return result;
    }

    @Override
    public PerColor clone() {
        PerColor perColor = new PerColor(this);
        return perColor;
    }
}
