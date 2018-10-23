package org.imdea.vcd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.ExceptionSet;
import org.imdea.vcd.queue.clock.MaxInt;

/**
 *
 * @author Vitor Enes
 */
public class Trace {

    public enum TraceType {
        COMMITTED(0),
        COMMIT(1);

        private final Integer id;

        TraceType(Integer id) {
            this.id = id;
        }

        public Integer getId() {
            return id;
        }
    }

    private static final String SEP = " ";

    private final TraceType type;
    private final Clock<ExceptionSet> committed;
    private final Dot dot;
    private final Clock<MaxInt> conf;

    public Trace(Clock<ExceptionSet> committed) {
        this.type = TraceType.COMMITTED;
        this.committed = committed;
        this.dot = null;
        this.conf = null;
    }

    public Trace(Dot dot, Clock<MaxInt> conf) {
        this.type = TraceType.COMMIT;
        this.committed = null;
        this.dot = dot;
        this.conf = conf;
    }

    public TraceType getType() {
        return type;
    }

    public Clock<ExceptionSet> getCommitted() {
        return committed;
    }

    public Dot getDot() {
        return dot;
    }

    public Clock<MaxInt> getConf() {
        return conf;
    }

    public static String encode(Clock<ExceptionSet> committed) {
        List<String> parts = new ArrayList<>();
        // serialize type
        parts.add(TraceType.COMMITTED.getId().toString());

        // serialize committed
        // format:
        // NODE_NUMBER [SEQ EXCEPTION_NUMBER [EXCEPTION]]
        // add NODE_NUMBER
        parts.add(Integer.toString(committed.size()));

        for (Integer id = 0; id < committed.size(); id++) {
            ExceptionSet exceptionSet = committed.get(id);
            HashSet<Long> exceptions = exceptionSet.getExceptions();

            // add SEQ
            parts.add(exceptionSet.current().toString());
            // add EXCEPTION_NUMBER
            parts.add(Integer.toString(exceptions.size()));
            // add [EXCEPTION]
            for (Long ex : exceptions) {
                parts.add(ex.toString());
            }
        }

        return String.join(SEP, parts);
    }

    public static String encode(Dot dot, Clock<MaxInt> conf) {
        List<String> parts = new ArrayList<>();
        // serialize type
        parts.add(TraceType.COMMIT.getId().toString());

        // serialize dot
        parts.add(dot.getId().toString());
        parts.add(dot.getSeq().toString());

        // serialize conf
        for (Integer id = 0; id < conf.size(); id++) {
            parts.add(conf.get(id).current().toString());
        }

        return String.join(SEP, parts);
    }

    public static Trace decode(String s) {
        String[] parts = s.split(SEP);
        Integer id = Integer.parseInt(parts[0]);

        if (id.equals(TraceType.COMMITTED.getId())) {
            return decodeCommitted(parts);
        } else if (id.equals(TraceType.COMMIT.getId())) {
            return decodeCommit(parts);
        } else {
            throw new RuntimeException("Trace type not found!");
        }

    }

    private static Trace decodeCommitted(String[] parts) {
        // decode node number
        Integer nodeNumber = Integer.parseInt(parts[1]);

        int i = 2;

        HashMap<Integer, ExceptionSet> map = new HashMap<>();
        for (Integer id = 0; id < nodeNumber; id++) {
            // decode seq
            Long seq = Long.parseLong(parts[i++]);

            // decode exception number
            Integer exceptionNumber = Integer.parseInt(parts[i++]);

            // decode exceptions
            HashSet<Long> exceptions = new HashSet<>();
            for (int j = 0; j < exceptionNumber; j++) {
                exceptions.add(Long.parseLong(parts[i++]));
            }

            ExceptionSet exceptionSet = new ExceptionSet(seq, exceptions);
            map.put(id, exceptionSet);
        }
        Clock<ExceptionSet> committed = new Clock<>(map);

        // create trace
        Trace trace = new Trace(committed);
        return trace;
    }

    private static Trace decodeCommit(String[] parts) {
        // decode dot
        Integer id = Integer.parseInt(parts[1]);
        Long seq = Long.parseLong(parts[2]);
        Dot dot = new Dot(id, seq);

        // decode conf
        HashMap<Integer, MaxInt> map = new HashMap<>();
        for (Integer i = 3; i < parts.length; i++) {
            map.put(i - 3, new MaxInt(Long.parseLong(parts[i])));
        }
        Clock<MaxInt> conf = new Clock<>(map);

        // create trace
        Trace trace = new Trace(dot, conf);
        return trace;
    }
}
