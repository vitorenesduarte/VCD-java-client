package org.imdea.vcd.queue;

import java.util.Collection;
import java.util.Set;
import java.util.TreeMap;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Dot;

/**
 *
 * @author Vitor Enes
 */
public class ConfQueueBox {

    private final TreeMap<Dot, Message> map;

    public ConfQueueBox(Dot dot, Message message) {
        this.map = new TreeMap<>();
        map.put(dot, message);
    }

    public ConfQueueBox(ConfQueueBox box) {
        this.map = new TreeMap<>(box.map);
    }

    public Set<Dot> getDots() {
        return this.map.keySet();
    }

    public void merge(ConfQueueBox o) {
        this.map.putAll(o.map);
    }

    public Collection<Message> sortMessages() {
        return this.map.values();
    }

    public int size() {
        return this.map.size();
    }

    @Override
    public String toString() {
        return this.map.keySet().toString();
    }

    @Override
    public Object clone() {
        ConfQueueBox committedQueueBox = new ConfQueueBox(this);
        return committedQueueBox;
    }
}
