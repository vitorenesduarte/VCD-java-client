package org.imdea.vcd.queue;

import java.util.HashSet;
import org.imdea.vcd.pb.Proto;

/**
 *
 * @author Vitor Enes
 */
public class ExceptionSet {

    private final Long seq;
    private final HashSet<Long> exceptions;

    public ExceptionSet(Long seq, HashSet<Long> exceptions) {
        this.seq = seq;
        this.exceptions = exceptions;
    }

    public ExceptionSet(Proto.ExceptionSet ex) {
        this(ex.getSeq(), new HashSet<>(ex.getExList()));
    }

    public boolean isElement(Long n) {
        return n <= this.seq && !this.exceptions.contains(n);
    }

    public static ExceptionSet merge(ExceptionSet a, ExceptionSet b) {
        Long seq = Long.max(a.seq, b.seq);

        HashSet<Long> exceptions = new HashSet<>();
        // collect exceptions from a
        for (Long s : a.exceptions) {
            if (s > b.seq || b.exceptions.contains(s)) {
                exceptions.add(s);
            }
        }

        // collect exceptions from b
        for (Long s : b.exceptions) {
            if (s > a.seq || a.exceptions.contains(s)) {
                exceptions.add(s);
            }
        }

        ExceptionSet c = new ExceptionSet(seq, exceptions);
        return c;
    }
}
