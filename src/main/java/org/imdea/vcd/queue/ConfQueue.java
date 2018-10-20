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

    private final HashMap<Dot, Vertex> vertexIndex = new HashMap<>();
    private List<ConfQueueBox> toDeliver = new ArrayList<>();

    private final Clock<ExceptionSet> delivered;

    public ConfQueue(Integer nodeNumber) {
        this.delivered = Clock.eclock(nodeNumber);
    }

    public ConfQueue(Clock<ExceptionSet> committed) {
        this(committed, true);
    }

    public ConfQueue(Clock<ExceptionSet> committed, boolean clone) {
        if (clone) {
            this.delivered = (Clock<ExceptionSet>) committed.clone();
        } else {
            this.delivered = committed;
        }
    }

    public boolean isEmpty() {
        return vertexIndex.isEmpty();
    }

    public void add(Dot dot, Message message, Clock<MaxInt> conf) {
        // create box
        ConfQueueBox box = new ConfQueueBox(dot, message, null);

        // create vertex
        Vertex v = new Vertex(conf, box);
        // update index
        vertexIndex.put(dot, v);

        if (delivered.contains(dot)) {
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
                Set<Dot> pending = new HashSet<>(vertexIndex.keySet());
                for (Dot next : pending) {
                    findSCC(next);
                }
            default:
                break;
        }
    }

    private void saveSCC(Dots scc) {
        // update delivered
        delivered.addDots(scc);

        // merge boxes of dots in SCC
        // - remove dot's box and conf along the way
        Iterator<Dot> it = scc.iterator();
        Dot member = it.next();
        ConfQueueBox merged = deleteMember(member);

        while (it.hasNext()) {
            member = it.next();
            ConfQueueBox box = deleteMember(member);
            merged.merge(box);
        }

        // add to toDeliver list
        toDeliver.add(merged);
    }

    private ConfQueueBox deleteMember(Dot member) {
        Vertex v = vertexIndex.remove(member);
        return v.box;
    }

    public List<ConfQueueBox> tryDeliver() {
        // return current list to be delivered,
        // and create a new one
        List<ConfQueueBox> result = toDeliver;
        toDeliver = new ArrayList<>();
        return result;
    }

    public int elements() {
        return vertexIndex.size();
    }

    private enum FinderResult {
        FOUND, NOT_FOUND, MISSING_DEP
    }

    private class Vertex {

        private final Clock<MaxInt> conf;
        private final ConfQueueBox box;

        public Vertex(Clock<MaxInt> conf, ConfQueueBox box) {
            this.conf = conf;
            this.box = box;
        }
    }

    /**
     * Find a SCC using Tarjan's algorithm.
     *
     * https://github.com/NYU-NEWS/janus/blob/09372bd1de206f9e0f15712a9a171a349bdcf3c0/src/deptran/rococo/graph.h#L274-L314
     *
     */
    private class TarjanSCCFinder {

        private final Deque<Dot> stack = new ArrayDeque<>();
        private final Map<Dot, Integer> ids = new HashMap<>();
        private final Map<Dot, Integer> low = new HashMap<>();
        private final Set<Dot> onStack = new HashSet<>();
        private Integer index = 0;

        private final List<Dots> sccs = new ArrayList<>();

        public FinderResult strongConnect(Dot v) {
            // find vertex
            Vertex vertex = vertexIndex.get(v);
            if (vertex == null) {
                return FinderResult.MISSING_DEP;
            }
            Clock<MaxInt> conf = vertex.conf;
            Integer N = conf.size();

            // add to the stack
            stack.push(v);
            onStack.add(v);
            // set id and low
            Integer vIndex = index;
            ids.put(v, vIndex);
            low.put(v, vIndex);
            // update id
            index++;

            // for all deps
            for (Integer i = 0; i < N; i++) {
                for (Long s = delivered.get(i).next(); s <= conf.get(i).current(); s++) {
                    Dot w = new Dot(i, s);
                    if (delivered.contains(w)) {
                        continue;
                    }
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
