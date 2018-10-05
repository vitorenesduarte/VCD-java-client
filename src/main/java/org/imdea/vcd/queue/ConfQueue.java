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
import java.util.Set;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.Dots;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.MaxInt;

/**
 *
 * @author Vitor Enes
 */
public class ConfQueue {

    // these two should always have the same size
    private final HashMap<Dot, ConfQueueBox> dotToBox = new HashMap<>();
    private final HashMap<Dot, Clock<MaxInt>> dotToConf = new HashMap<>();
    private List<ConfQueueBox> toDeliver = new ArrayList<>();

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

    public boolean isEmpty() {
        return this.dotToConf.isEmpty();
    }

    public void add(Dot dot, Message message, Clock<MaxInt> conf) {
        // create box
        ConfQueueBox box = new ConfQueueBox(dot, message, conf);

        // update committed
        this.committed.addDot(dot);

        // save box
        this.dotToBox.put(dot, box);
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
        // if not committed or already delivered, return
        if (!this.committed.contains(dot) || this.delivered.contains(dot)) {
            return;
        }

        TarjanSCCFinder finder = new TarjanSCCFinder();
        FinderResult res = finder.strongConnect(dot);
        switch (res) {
            case FOUND:
                List<Dots> sccs = finder.getSCCs();

                // deliver all sccs by the order they were found
                for (Dots scc : sccs) {
                    saveSCC(scc);
                }

                // try to deliver the next dots after delivered
                for (Dot next : this.delivered.nextDots()) {
                    findSCC(next);
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
        ConfQueueBox merged = new ConfQueueBox(this.committed.size());
        Iterator<Dot> it = scc.iterator();
        do {
            Dot member = it.next();
            ConfQueueBox box = resetMember(member);
            merged.merge(box);
        } while (it.hasNext());

        // add to toDeliver list
        this.toDeliver.add(merged);
    }

    private ConfQueueBox resetMember(Dot member) {
        this.dotToConf.remove(member);
        return this.dotToBox.remove(member);
    }

    public List<ConfQueueBox> tryDeliver() {
        // return current list to be delivered,
        // and create a new one
        List<ConfQueueBox> result = this.toDeliver;
        this.toDeliver = new ArrayList<>();
        return result;
    }

    public int elements() {
        return this.dotToConf.size();
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
        private final Set<Dot> onStack;
        private Integer index;

        private final List<Dots> sccs;

        public TarjanSCCFinder() {
            this.stack = new ArrayDeque<>();
            this.ids = new HashMap<>();
            this.low = new HashMap<>();
            this.onStack = new HashSet<>();
            this.index = 0;
            this.sccs = new ArrayList();
        }

        public FinderResult strongConnect(Dot v) {
            // get conf
            Clock<MaxInt> conf = dotToConf.get(v);

            // get neighbors: subtract delivered
            Dots deps = new Dots();

            // if not all deps are committed, give up
            for (Dot dep : conf.frontier(v)) {
                if (!committed.contains(dep)) {
                    return FinderResult.MISSING_DEP;
                }
                if (!delivered.contains(dep)) {
                    deps.add(dep);
                }
            }

            // add to the stack
            stack.push(v);
            onStack.add(v);
            // set id and low
            Integer vIndex = index;
            ids.put(v, vIndex);
            low.put(v, vIndex);
            // update id
            index++;

            // for all neighbors
            for (Dot w : deps) {
                // if not visited, visit
                boolean visited = ids.containsKey(w);
                if (!visited) {
                    FinderResult result = strongConnect(w);

                    switch (result) {
                        case MISSING_DEP:
                            // propagate missing dep
                            return result;
                        default:
                            break;
                    }
                    low.put(v, Math.min(low.get(v), low.get(w)));

                } // if visited neighbor is on stack, min lows
                else if (onStack.contains(w)) {
                    low.put(v, Math.min(low.get(v), ids.get(w)));
                }
            }

            // if after visiting all neighbors, an SCC was found if
            // good news: the SCC members are in the stack
            if (Objects.equals(vIndex, low.get(v))) {
                Dots scc = new Dots();

                for (Dot w = stack.pop();; w = stack.pop()) {
                    // remove from stack
                    onStack.remove(w);
                    // add to SCC
                    scc.add(w);

                    // exit if done
                    if (w.equals(v)) {
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
