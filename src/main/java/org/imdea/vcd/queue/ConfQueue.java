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

    private static final boolean TRANSITIVE = true;

    private final HashMap<Dot, Vertex> vertexIndex = new HashMap<>();
    private final HashMap<Dot, Dots> childrenIndex = new HashMap<>();
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
        Vertex vertex = new Vertex(dot, message, conf);
        // update indexes
        updateIndexes(dot, vertex, conf);

        if (delivered.contains(dot)) {
            // FOR THE CASE OF FAILURES: just deliver again?
            // this shouldn't happen
            Dots scc = new Dots();
            scc.add(dot);
            saveSCC(scc);
        } else {
            // try to find a SCC
            findSCC(dot, vertex);
        }
    }

    private void updateIndexes(Dot dot, Vertex vertex, Clock<MaxInt> conf) {
        // update vertex index
        vertexIndex.put(dot, vertex);

        // update children index
        Dots highestDeps = conf.frontier();
        for (Dot dep : highestDeps) {
            Dots children = childrenIndex.get(dep);

            // if this dep already has children, simply add a new one
            // otherwise, create children, add, and update index
            if (children != null) {
                children.add(dot);
            } else {
                children = new Dots();
                children.add(dot);
                childrenIndex.put(dep, children);
            }
        }
    }

    private void findSCC(Dot dot, Vertex vertex) {
        TarjanSCCFinder finder = new TarjanSCCFinder();
        FinderResult res = finder.strongConnect(dot, vertex);

        // reset ids of stack
        Dots stack = finder.getStack();
        for (Dot d : stack) {
            vertexIndex.get(d).id = 0;
        }

        // check if SCCs were found
        switch (res) {
            case FOUND:
                List<Dots> sccs = finder.getSCCs();
                for (Dots scc : sccs) {
                    saveSCC(scc);
                }
                tryPendingChildren(sccs);
            default:
                break;
        }
    }

    private void saveSCC(Dots scc) {
        // update delivered
        delivered.addDots(scc);

        // merge all boxes in SCC
        // - remove from index along the way
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

    private void tryPendingChildren(List<Dots> sccs) {
        Dots visited = new Dots();
        for (Dots scc : sccs) {
            for (Dot dot : scc) {
                Dots children = childrenIndex.remove(dot);
                if (children != null) {
                    for (Dot child : children) {
                        Vertex vertex = vertexIndex.get(child);
                        if (vertex != null && !visited.contains(child)) {
                            visited.add(child);
                            findSCC(child, vertex);
                        }
                    }
                }
            }
        }
    }

    public List<ConfQueueBox> getToDeliver() {
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
        private Integer id;
        private Integer low;

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
        private final Dots onStack = new Dots();
        private Integer id = 0;

        private final List<Dots> sccs = new ArrayList<>();

        public FinderResult strongConnect(Dot vd, Vertex v) {
            // get conf
            Clock<MaxInt> conf = v.conf;

            // add to the stack
            stack.push(vd);
            onStack.add(vd);
            // update id
            id++;
            // set id and low
            v.id = id;
            v.low = id;

            // for all deps
            for (Integer q = 0; q < N; q++) {
                Long to = conf.get(q).current();
                Long from;
                if (TRANSITIVE) {
                    from = to;
                } else {
                    from = delivered.get(q).next();
                }
                // start from dep to del (assuming we would give up, we give up faster this way)
                for (; to >= from; to--) {
                    Dot wd = new Dot(q, to);

                    // ignore delivered
                    // - we need to check delivered since the clock has exceptions
                    if (delivered.contains(wd)) {
                        continue;
                    }

                    // find vertex
                    Vertex w = vertexIndex.get(wd);
                    if (w == null) {
                        // NOT NECESSARILY A MISSING DEP (SINCE IT MIGHT NOT CONFLICT)
                        // BUT WE CAN'T KNOW UNTIL WE SEE IT
                        return FinderResult.MISSING_DEP;
                    }

                    // ignore non-conflicting
                    if (!TRANSITIVE && !v.conflict(w)) {
                        continue;
                    }

                    // if not visited, visit
                    if (w.id == 0) {
                        FinderResult result = strongConnect(wd, w);

                        switch (result) {
                            case MISSING_DEP:
                                // propagate missing dep
                                return result;
                            default:
                                break;
                        }
                        v.low = Math.min(v.low, w.low);

                    } // if visited neighbor is on stack, min lows
                    else if (onStack.contains(wd)) {
                        v.low = Math.min(v.low, w.id);
                    }
                }
            }

            // if after visiting all neighbors, an SCC was found if
            // good news: the SCC members are in the stack
            if (Objects.equals(v.id, v.low)) {
                Dots scc = new Dots();
                for (Dot d = stack.pop();; d = stack.pop()) {
                    // remove from stack
                    onStack.remove(d);
                    // add to SCC
                    scc.add(d);

                    // exit if done
                    if (d.equals(vd)) {
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

        private Dots getStack() {
            return this.onStack;
        }
    }
}
