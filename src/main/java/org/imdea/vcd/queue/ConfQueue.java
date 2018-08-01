package org.imdea.vcd.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.imdea.vcd.queue.box.QueueBox;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.Dots;
import org.imdea.vcd.queue.clock.ExceptionSet;

/**
 *
 * @author Vitor Enes
 * @param <E>
 */
public class ConfQueue<E extends QueueBox> implements Queue<E> {

    // these two should always have the same size
    private final HashMap<Dot, E> dotToBox = new HashMap<>();
    private final HashMap<Dot, Clock<ExceptionSet>> dotToConf = new HashMap<>();
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
        return this.dotToConf.isEmpty();
    }

    @Override
    public void add(QueueAddArgs<E> args) {
        // fetch box, dot and conf
        E e = args.getBox();
        Dot dot = args.getDot();
        Clock<ExceptionSet> conf = Clock.eclock(args.getConf());

        // update committed
        this.committed.addDot(dot);

        // save box
        this.dotToBox.put(dot, e);
        // save info
        this.dotToConf.put(dot, conf);

        // try to find an SCC
        TarjanSCCFinder finder = new TarjanSCCFinder();
        Dots scc = finder.dfs(dot);
        if (scc != null) {
            // update delivered
            this.delivered.addDots(scc);

            // merge boxes of dots in SCC
            // - remove dot's box and conf along the way
            Iterator<Dot> it = scc.iterator();
            Dot member = it.next();
            E box = this.dotToBox.remove(member);
            this.dotToConf.remove(member);

            while (it.hasNext()) {
                member = it.next();
                box.merge(this.dotToBox.remove(member));
                this.dotToConf.remove(member);
            }

            // add to toDeliver list
            this.toDeliver.add(box);
        }
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
        return this.dotToConf.size();
    }

    @Override
    public int elements() {
        return size();
    }

    /**
     * Find a SCC using Tarjan's algorithm.
     *
     * Based on
     * https://github.com/williamfiset/Algorithms/blob/master/com/williamfiset/algorithms/graphtheory/TarjanSccSolverAdjacencyList.java
     *
     */
    private class TarjanSCCFinder {

        private final Deque<Dot> stack;
        private final Map<Dot, Integer> ids;
        private final Map<Dot, Integer> low;
        private final Map<Dot, Boolean> onStack;
        private Integer id;

        public TarjanSCCFinder() {
            this.stack = new ArrayDeque<>();
            this.ids = new HashMap<>();
            this.low = new HashMap<>();
            this.onStack = new HashMap<>();
            this.id = 0;
        }

        public Dots dfs(Dot at) {
            // get conf
            Clock<ExceptionSet> conf = dotToConf.get(at);

            // if not all deps are committed, give up
            if (!allDepsCommitted(conf)) {
                return null;
            }

            // add to the stack
            stack.push(at);
            onStack.put(at, true);
            // set id and low
            Integer atId = id;
            ids.put(at, atId);
            low.put(at, atId);
            // update id
            id++;

            // get neighbors
            Dots deps = conf.subtract(delivered);

            // for all neighbors
            for (Dot to : deps) {
                // if not visited, visit
                boolean visited = ids.containsKey(to);
                if (!visited) {
                    dfs(to);
                }

                // if visited neighbor is on stack, min lows
                Boolean stacked = onStack.get(to);
                if (stacked != null && stacked) {
                    // low[at] = min(low[at], low[to])
                    Integer newLow = Math.min(low.get(at), low.get(to));
                    low.put(at, newLow);
                }
            }

            // if after visiting all neighbors, and SCC was found if
            // id[at] == low[at]
            // good news: the SCC members are in the stack
            if (Objects.equals(atId, low.get(at))) {
                Dots scc = new Dots();

                for (Dot member = stack.pop();; member = stack.pop()) {
                    // remove from stack
                    onStack.remove(member);
                    // update low
                    low.put(member, atId);
                    // add to SCC
                    scc.add(member);

                    // exit if done
                    if (member.equals(at)) {
                        break;
                    }
                }

                return scc;
            } else {
                return null;
            }
        }

        private boolean allDepsCommitted(Clock<ExceptionSet> conf) {
            Dots missingDeps = conf.subtract(committed);
            return missingDeps.isEmpty();
        }
    }
}
