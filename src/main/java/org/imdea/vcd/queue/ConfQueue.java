package org.imdea.vcd.queue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
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
    private final HashSet<Dot> childrenIsSet = new HashSet<>();
    private final HashMap<Dot, Dots> dotToChildren = new HashMap<>();
    private List<E> toDeliver = new ArrayList<>();

    private final Clock<ExceptionSet> committed;
    private final Clock<ExceptionSet> delivered;

    public ConfQueue(Integer nodeNumber) {
        this.committed = Clock.eclock(nodeNumber);
        this.delivered = Clock.eclock(nodeNumber);
    }

    public ConfQueue(Clock<ExceptionSet> committed) {
        this.committed = (Clock<ExceptionSet>) committed.clone();
        this.delivered = (Clock<ExceptionSet>) committed.clone();
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

        if (this.delivered.contains(dot)) {
            // FOR THE CASE OF FAILURES: just deliver again?
            // this probably shouldn't happen
            Dots scc = new Dots();
            scc.add(dot);
            saveSCC(scc);
            return;
        }

        // try to find an SCC
        findSCC(dot);
    }

    private void findSCC(Dot dot) {
        // if already delivered, return
        if (this.delivered.contains(dot)) {
            return;
        }

        TarjanSCCFinder finder = new TarjanSCCFinder();
        FinderResult res = finder.dfs(dot);
        switch (res) {
            case FOUND:
                List<Dots> sccs = finder.getSCCs();
                Dots toBeDelivered = new Dots();

                // deliver all sccs by the order they were found
                for (Dots scc : sccs) {
                    saveSCC(scc);
                    toBeDelivered.merge(scc);
                }

                // and try to deliver more, based on the
                // children of the dots delivered
                for (Dot del : toBeDelivered) {
                    this.childrenIsSet.remove(del);
                    Dots children = this.dotToChildren.remove(del);
                    if (children != null) {
                        for (Dot child : children) {
                            findSCC(child);
                        }
                    }
                }
            default:
                break;
        }
    }

    private void saveSCC(Dots scc) {
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

    private enum FinderResult {
        FOUND, NOT_FOUND, MISSING_DEP
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

        private final List<Dots> sccs;

        public TarjanSCCFinder() {
            this.stack = new ArrayDeque<>();
            this.ids = new HashMap<>();
            this.low = new HashMap<>();
            this.onStack = new HashMap<>();
            this.id = 0;
            this.sccs = new ArrayList();
        }

        public FinderResult dfs(Dot at) {
            // get conf
            Clock<ExceptionSet> conf = dotToConf.get(at);

            // get neighbors: subtract delivered and self
            Dots deps = conf.subtract(delivered);
            deps.remove(at);

            // check if children is already set
            if (!childrenIsSet.contains(at)) {

                for (Dot missingDep : deps) {
                    Dots children = dotToChildren.get(missingDep);
                    if (children == null) {
                        children = new Dots();
                    }
                    children.add(at);
                    dotToChildren.put(missingDep, children);
                }

                childrenIsSet.add(at);
            }

            // if not all deps are committed, give up
            Dots missingDeps = conf.subtract(committed);
            if (!missingDeps.isEmpty()) {
                return FinderResult.MISSING_DEP;
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

            // for all neighbors
            for (Dot to : deps) {
                // if not visited, visit
                boolean visited = ids.containsKey(to);
                if (!visited) {
                    FinderResult result = dfs(to);

                    switch (result) {
                        case MISSING_DEP:
                            // propagate missing dep
                            return result;
                        default:
                            break;
                    }
                }

                // if visited neighbor is on stack, min lows
                Boolean stacked = onStack.get(to);
                if (stacked != null && stacked) {
                    // low[at] = min(low[at], low[to])
                    Integer newLow = Math.min(low.get(at), low.get(to));
                    low.put(at, newLow);
                }
            }

            // if after visiting all neighbors, an SCC was found if
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

                // add scc to the set of sccs
                sccs.add(scc);

                return FinderResult.FOUND;
            }

            return FinderResult.NOT_FOUND;
        }

        private List<Dots> getSCCs() {
            return this.sccs;
        }
    }
}
