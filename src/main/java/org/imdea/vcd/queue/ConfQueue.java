package org.imdea.vcd.queue;

import com.google.protobuf.ByteString;
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
import java.util.logging.Logger;
import org.imdea.vcd.VCDLogger;
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

    private static final Logger LOGGER = VCDLogger.init(ConfQueue.class);

    private final HashMap<Dot, Vertex> vertexIndex = new HashMap<>();
    private List<ConfQueueBox> toDeliver = new ArrayList<>();

    private final Clock<ExceptionSet> delivered;
    private final Integer N;

    public ConfQueue(Integer nodeNumber) {
        this.delivered = Clock.eclock(nodeNumber);
        this.N = nodeNumber;
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
        this.N = this.delivered.size();
    }

    public boolean isEmpty() {
        return vertexIndex.isEmpty();
    }

    public void add(Dot dot, Message message, Clock<MaxInt> conf) {
        // create vertex
        Vertex v = new Vertex(dot, message, conf);
        // update index
        vertexIndex.put(dot, v);

        if (delivered.contains(dot)) {
            // FOR THE CASE OF FAILURES: just deliver again?
            // this shouldn't happen
            Dots scc = new Dots();
            scc.add(dot);
            saveSCC(scc);
        } else {
            // try to find an SCC
            if (findSCC(dot, v)) {
                tryPending();
            }
        }
    }

    private boolean tryPending() {
        HashMap<Dot, Vertex> pending = new HashMap<>(vertexIndex);
        // this can be optimized
        // - if two pending messages can't be delivered (missing dep)
        // and are part of the same SCC, both will be traversed twice,
        // one for each pending message
        for (Map.Entry<Dot, Vertex> e : pending.entrySet()) {
            if (findSCC(e.getKey(), e.getValue())) {
                return tryPending();
            }
        }
        return true;
    }

    public Clock<ExceptionSet> getDelivered() {
        return delivered;
    }

    private boolean findSCC(Dot dot, Vertex vertex) {
        TarjanSCCFinder finder = new TarjanSCCFinder();
        FinderResult res = finder.strongConnect(dot, vertex);
        switch (res) {
            case FOUND:
                for (Dots scc : finder.getSCCs()) {
                    saveSCC(scc);
                }
                return true;
            default:
                return false;
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
        private final HashSet<ByteString> colors;

        public Vertex(Dot dot, Message message, Clock<MaxInt> conf) {
            this.conf = conf;
            this.colors = new HashSet<>(message.getHashesList());
            this.box = new ConfQueueBox(dot, message, null);
        }

        public boolean conflict(Vertex v) {
            for (ByteString s : colors) {
                if (v.colors.contains(s)) {
                    return true;
                }
            }
            return false;
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

        public FinderResult strongConnect(Dot v, Vertex vVertex) {
            Clock<MaxInt> conf = vVertex.conf;

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
            for (Integer q = 0; q < N; q++) {
                Long dep = conf.get(q).current();
                Long nextDel = delivered.get(q).next();
                // start from dep to del (assuming we would give up, we give up faster this way)
                for (; dep >= nextDel; dep--) {
                    Dot w = new Dot(q, dep);

                    // ignore self and delivered
                    // - we need to check delivered singe it has exceptions
                    if (w.equals(v) || delivered.contains(w)) {
                        continue;
                    }

                    // find vertex
                    Vertex wVertex = vertexIndex.get(w);
                    if (wVertex == null) {
                        // NOT NECESSARILY BUT WE CAN'T KNOW UNTIL WE SEE IT
                        return FinderResult.MISSING_DEP;
                    }

                    // ignore non-conflicting
                    if (!vVertex.conflict(wVertex)) {
                        continue;
                    }

                    // if not visited, visit
                    boolean visited = ids.containsKey(w);
                    if (!visited) {
                        FinderResult result = strongConnect(w, wVertex);

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

                if (!stack.isEmpty()) {
                    // this occurs several times
                    // and epaxos takes the whole stack, which is wrong
                }

                return FinderResult.FOUND;
            }

            return FinderResult.NOT_FOUND;
        }

        private List<Dots> getSCCs() {
            return this.sccs;
        }
    }
}
