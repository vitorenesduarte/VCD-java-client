package org.imdea.vcd.queue.clock;

import java.util.HashSet;
import org.imdea.vcd.pb.Proto;

/**
 *
 * @author Vitor Enes
 */
public class ExceptionSet implements IntSet<ExceptionSet> {

    private Long seq;
    private HashSet<Long> exceptions;

    public ExceptionSet(Long seq, HashSet<Long> exceptions) {
        this.seq = seq;
        this.exceptions = exceptions;
    }

    public ExceptionSet(Proto.ExceptionSet ex) {
        this(ex.getSeq(), new HashSet<>(ex.getExList()));
    }

    @Override
    public boolean contains(Long seq) {
        return seq <= this.seq && !this.exceptions.contains(seq);
    }

    @Override
    public void merge(ExceptionSet o) {
        ExceptionSet merged = merge(this, o);
        this.seq = merged.seq;
        this.exceptions = merged.exceptions;
    }

    private ExceptionSet merge(ExceptionSet a, ExceptionSet b) {
        Long newSeq = Long.max(a.seq, b.seq);

        HashSet<Long> newExceptions = new HashSet<>();
        // collect exceptions from a
        for (Long s : a.exceptions) {
            if (s > b.seq || b.exceptions.contains(s)) {
                newExceptions.add(s);
            }
        }

        // collect exceptions from b
        for (Long s : b.exceptions) {
            if (s > a.seq || a.exceptions.contains(s)) {
                newExceptions.add(s);
            }
        }

        ExceptionSet c = new ExceptionSet(newSeq, newExceptions);
        return c;
    }
}
