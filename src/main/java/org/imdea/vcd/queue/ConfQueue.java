package org.imdea.vcd.queue;

import org.imdea.vcd.queue.box.PerMessage;
import org.imdea.vcd.queue.box.QueueBox;
import java.util.HashMap;
import java.util.List;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.ExceptionSet;

/**
 *
 * @author Vitor Enes
 * @param <E>
 */
public class ConfQueue<E extends QueueBox> implements Queue<E> {

    private HashMap<Dot, PerMessage> dotMap;

    private Clock<ExceptionSet> delivered;

    public ConfQueue(Integer nodeNumber) {
        this.delivered = Clock.eclock(nodeNumber);
        this.dotMap = new HashMap<>();
    }

    public ConfQueue(Clock<ExceptionSet> delivered) {
        this.delivered = delivered;
        this.dotMap = new HashMap<>();
    }

    @Override
    public boolean isEmpty() {
        return this.dotMap.isEmpty();
    }

    @Override
    public void add(E e) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<E> tryDeliver() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<E> toList() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int size() {
        return this.dotMap.size();
    }

    @Override
    public int elements() {
        return size();
    }
}
