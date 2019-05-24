package org.imdea.vcd.queue;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.*;
import org.imdea.vcd.metrics.ClientMetrics;

import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.Dots;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.MaxInt;

/**
 * @author Vitor Enes
 */
public class ConfQueue {

    private final HashMap<Dot, Vertex> vertexIndex = new HashMap<>();
    private final HashMap<ByteString, HashSet<Vertex>> pendingIndex = new HashMap<>();
    private List<ConfQueueBox> toDeliver = new ArrayList<>();

    private final Clock<ExceptionSet> delivered;
    private final Integer N;
    private final boolean TRANSITIVE;
    private final boolean OPT_DELIVERY;

    public ConfQueue(Integer nodeNumber, boolean batching, boolean optDelivery) {
        this(Clock.eclock(nodeNumber), batching, optDelivery);
    }

    public ConfQueue(Clock<ExceptionSet> committed, boolean batching, boolean optDelivery) {
//        ClientMetrics.queue("init;" + System.currentTimeMillis() + ";" + committed);
        this.delivered = (Clock<ExceptionSet>) committed.clone();
        this.N = this.delivered.size();
        this.TRANSITIVE = isTransitive(batching);
        this.OPT_DELIVERY = optDelivery;
    }

    private boolean isTransitive(boolean batching) {
        // with batching (or any application in which operations are multi-key),
        // the conflict relation is not transitive
        return !batching;
    }

    public boolean isEmpty() {
        return vertexIndex.isEmpty();
    }

    public void add(Dot dot, Message message, Clock<MaxInt> conf) throws InvalidProtocolBufferException {
        // create vertex
        Vertex vertex = new Vertex(dot, message, conf);
//        ClientMetrics.queue("add;" + System.currentTimeMillis() + ";" + vertex.dot + ";" + Arrays.toString(vertex.colors.iterator().next().toByteArray()) + ";" + vertex.conf);

        // update indexes
        updateIndexes(dot, vertex);

        // try to find a SCC
        Dots visited = new Dots();
        Collection<ByteString> colors = findSCC(dot, vertex, visited);
        tryPending(colors);
    }

    private void updateIndexes(Dot dot, Vertex vertex) {
        vertexIndex.put(dot, vertex);

        for (ByteString color : vertex.colors) {
            HashSet<Vertex> pending = pendingIndex.get(color);

            if (pending == null) {
                // if not found for this color, create it
                pending = new HashSet<>();
                pending.add(vertex);
                pendingIndex.put(color, pending);
            } else {
                // otherwise, just update it
                pending.add(vertex);
            }
        }
    }

    private Collection<ByteString> findSCC(Dot dot, Vertex vertex, Dots visited) {
        TarjanSCCFinder finder = new TarjanSCCFinder();
        FinderResult res = finder.strongConnect(dot, vertex);

        // reset ids of stack
        for (Vertex s : finder.getStack()) {
            s.id = 0;
            visited.add(s.dot);
        }

        // create set of delivered colors
        HashSet<ByteString> colors = new HashSet<>();

        // check if SCCs were found
        switch (res) {
            case FOUND:
                List<Dots> sccs = finder.getSCCs();
                for (Dots scc : sccs) {
                    visited.addAll(scc);
                    saveSCC(scc, colors);
                }
                break;
            default:
                break;
        }

        return colors;
    }

    private void saveSCC(Dots scc, HashSet<ByteString> colors) {
        // update delivered
        delivered.addDots(scc);

        // merge all boxes in SCC
        // - remove from index along the way
        Iterator<Dot> it = scc.iterator();
        Dot member = it.next();
        ConfQueueBox merged = deleteMember(member, colors);

        while (it.hasNext()) {
            member = it.next();
            ConfQueueBox box = deleteMember(member, colors);
            merged.merge(box);
        }

        // add to toDeliver list
        toDeliver.add(merged);
    }

    private ConfQueueBox deleteMember(Dot member, HashSet<ByteString> colors) {
        Vertex vertex = vertexIndex.remove(member);
//        ClientMetrics.queue("rmv;" + System.currentTimeMillis() + ";" + vertex.dot + ";" + Arrays.toString(vertex.colors.iterator().next().toByteArray()) + ";" + vertex.conf);

        // update set of delivered colors
        colors.addAll(vertex.colors);
        // update pending index
        for (ByteString color : vertex.colors) {
            pendingIndex.get(color).remove(vertex);
        }
        // return box
        return vertex.box;
    }

    public void tryPending(Collection<ByteString> colors) {
        Dots visited = new Dots();
        if (OPT_DELIVERY) {
            for (ByteString color : colors) {
                HashSet<Vertex> pending = new HashSet<>(pendingIndex.get(color));
                for (Vertex v : pending) {
                    Dot d = v.dot;
                    if (!visited.contains(d)) {
                        findSCC(d, v, visited);
                    }
                }
            }
        } else {
            for (Dot d : delivered.nextDots()) {
                Vertex v = vertexIndex.get(d);
                if (v != null) {
                    findSCC(d, v, visited);
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

        private final Dot dot;
        private final Clock<MaxInt> conf;
        private final ConfQueueBox box;
        private final HashSet<ByteString> colors;
        private Integer id;
        private Integer low;
        private Boolean onStack;

        public Vertex(Dot dot, Message message, Clock<MaxInt> conf) throws InvalidProtocolBufferException {
            this.dot = dot;
            this.conf = conf;
            this.colors = new HashSet<>(message.getHashesList());
            this.box = new ConfQueueBox(dot, message);
            this.onStack = false;
        }

        public boolean conflict(Vertex v) {
            for (ByteString s : colors) {
                if (v.colors.contains(s)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return this.dot.hashCode();
        }
    }

    /**
     * Find a SCC using Tarjan's algorithm.
     * <p>
     * https://github.com/NYU-NEWS/janus/blob/09372bd1de206f9e0f15712a9a171a349bdcf3c0/src/deptran/rococo/graph.h#L274-L314
     */
    private class TarjanSCCFinder {

        private final Deque<Vertex> stack = new ArrayDeque<>();
        private Integer id = 0;

        private final List<Dots> sccs = new ArrayList<>();

        public FinderResult strongConnect(Dot vd, Vertex v) {
            // get conf
            Clock<MaxInt> conf = v.conf;

            // update id
            id++;
            // set id and low
            v.id = id;
            v.low = id;
            v.onStack = true;

            // add to the stack
            stack.push(v);

            // for all deps
            for (Integer q = 0; q < N; q++) {
                Long to = conf.get(q).current();
                Long from;
                if (OPT_DELIVERY && TRANSITIVE) {
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

                    // ignore non-conflicting commands
                    if (OPT_DELIVERY && !TRANSITIVE && !v.conflict(w)) {
                        // if transitive, then it conflicts for sure
                        // since we're only checking the highest dep
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
                    else if (w.onStack) {
                        v.low = Math.min(v.low, w.id);
                    }
                }
            }

            // if after visiting all neighbors, an SCC was found if
            // good news: the SCC members are in the stack
            if (Objects.equals(v.id, v.low)) {
                Dots scc = new Dots();

                for (Vertex s = stack.pop();; s = stack.pop()) {
                    // remove from stack
                    s.onStack = false;

                    // add to SCC
                    scc.add(s.dot);

                    // exit if done
                    if (s.dot.equals(vd)) {
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

        private Collection<Vertex> getStack() {
            return this.stack;
        }
    }
}
