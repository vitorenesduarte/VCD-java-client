package org.imdea.vcd.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.Dots;

/**
 *
 * @author Vitor Enes
 */
public class DeliveredDepBox implements DepBox<DeliveredDepBox> {

    private final Dots dots;
    private final Clock<ExceptionSet> dep;
    private final TreeMap<Dot, Message> messageMap;

    public DeliveredDepBox(PerMessage perMessage) {
        this.dots = new Dots(perMessage.getDot());
        this.dep = Clock.eclock(perMessage.getConf());
        this.messageMap = new TreeMap<>();
        this.messageMap.put(perMessage.getDot(), perMessage.getMessage());
    }

    @Override
    public boolean before(DeliveredDepBox o) {
        return o.dep.intersects(this.dots);
    }

    @Override
    public void merge(DeliveredDepBox o) {
        this.dots.merge(o.dots);
        this.dep.merge(o.dep);
        for (Map.Entry<Dot, Message> entry : o.messageMap.entrySet()) {
            if (this.messageMap.containsKey(entry.getKey())) {
                throw new RuntimeException("Duplicated dot " + entry.getKey());
            }
            this.messageMap.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean canDeliver(Clock<ExceptionSet> delivered) {
        // delivered will be mutated, adding the dots from this box
        // - in case it can deliver, delivered will be the next queue clock
        delivered.addDots(this.dots);
        return delivered.equals(this.dep);
    }

    public Dots getDots() {
        return this.dots;
    }

    public List<Message> sortMessages() {
        List<Message> result = new ArrayList<>();
        for (Message message : messageMap.values()) {
            result.add(message);
        }
        return result;
    }

    @Override
    public String toString() {
        return dots + " " + dep;
    }
}
