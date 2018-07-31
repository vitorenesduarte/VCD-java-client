package org.imdea.vcd.queue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.imdea.vcd.pb.Proto.Commit;
import org.imdea.vcd.queue.box.QueueBox;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.ExceptionSet;

/**
 *
 * @author Vitor Enes
 * @param <E>
 */
public class ConfQueue<E extends QueueBox> implements Queue<E> {

    // these two should always have the same size
    private final HashMap<Dot, E> dotToBox = new HashMap<>();
    private final HashMap<Dot, Clock<ExceptionSet>> dotToInfo = new HashMap<>();
    private List<E> toDeliver = new ArrayList<>();

    private final Clock<ExceptionSet> committed;
    private final Clock<ExceptionSet> delivered;

    public ConfQueue(Integer nodeNumber) {
        this.committed = Clock.eclock(nodeNumber);
        this.delivered = Clock.eclock(nodeNumber);
    }

    public ConfQueue(Clock<ExceptionSet> delivered) {
        this.committed = Clock.eclock(delivered.size());
        this.delivered = delivered;
    }

    @Override
    public boolean isEmpty() {
        return this.dotToInfo.isEmpty();
    }

    @Override
    public void add(E e, Commit commit) {
        // fetch dot
        Dot dot = Dot.dot(commit.getDot());
        // create per message
        Clock<ExceptionSet> conf = Clock.eclock(Clock.vclock(commit.getConfMap()));

        // save box
        this.dotToBox.put(dot, e);
        // save info
        this.dotToInfo.put(dot, conf);

    }

    @Override
    public List<E> tryDeliver() {
        // return current list to be delivered,
        // and create a new one
        List<E> result = this.toDeliver;
        this.toDeliver = new ArrayList<>();
        return result;
    }

    @Override
    public List<E> toList() {
        throw new UnsupportedOperationException("Method not supported.");
    }

    @Override
    public int size() {
        return this.dotToInfo.size();
    }

    @Override
    public int elements() {
        return size();
    }
}
