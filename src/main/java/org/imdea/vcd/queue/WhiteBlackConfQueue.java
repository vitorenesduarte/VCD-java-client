package org.imdea.vcd.queue;

import java.util.ArrayList;
import java.util.List;
import org.imdea.vcd.Generator;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.MaxInt;

/**
 *
 * @author Vitor Enes
 */
public class WhiteBlackConfQueue {

    private final Clock<ExceptionSet> delivered;
    private final ConfQueue white;
    private final ConfQueue black;
    private List<ConfQueueBox> toDeliver = new ArrayList<>();

    public WhiteBlackConfQueue(Integer nodeNumber) {
        this(Clock.eclock(nodeNumber));
    }

    public WhiteBlackConfQueue(Clock<ExceptionSet> committed) {
        this.delivered = (Clock<ExceptionSet>) committed.clone();
        this.white = new ConfQueue(delivered, false);
        this.black = new ConfQueue(delivered, false);
    }

    public boolean isEmpty() {
        return white.isEmpty() && black.isEmpty();
    }

    public void add(Dot dot, Message message, Clock<MaxInt> conf) {
        ConfQueue queue;

        if (message.getHashes(0).equals(Generator.BLACK)) {
            queue = black;
        } else {
            queue = white;
        }

        queue.add(dot, message, conf);
        List<ConfQueueBox> l = queue.getToDeliver();
        toDeliver.addAll(l);
    }

    public List<ConfQueueBox> getToDeliver() {
        // return current list to be delivered,
        // and create a new one
        List<ConfQueueBox> result = toDeliver;
        toDeliver = new ArrayList<>();
        return result;
    }

    public int elements() {
        return white.elements() + black.elements();
    }
}
