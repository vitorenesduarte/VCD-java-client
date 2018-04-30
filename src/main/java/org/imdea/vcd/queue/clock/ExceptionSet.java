package org.imdea.vcd.queue.clock;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import org.imdea.vcd.pb.Proto;

/**
 *
 * @author Vitor Enes
 */
public class ExceptionSet implements IntSet<ExceptionSet> {

    private Long seq;
    private HashSet<Long> exceptions;

    public ExceptionSet() {
        this.seq = 0L;
        this.exceptions = new HashSet<>();
    }

    public ExceptionSet(Long seq, HashSet<Long> exceptions) {
        this.seq = seq;
        this.exceptions = exceptions;
    }

    public ExceptionSet(Long seq, Long... exceptions) {
        this.seq = seq;
        this.exceptions = new HashSet<>(Arrays.asList(exceptions));
    }

    public ExceptionSet(Proto.ExceptionSet ex) {
        this(ex.getSeq(), new HashSet<>(ex.getExList()));
    }

    public MaxInt toMaxInt() {
        return new MaxInt(this.seq);
    }

    @Override
    public boolean contains(Long seq) {
        return seq <= this.seq && !this.exceptions.contains(seq);
    }

    @Override
    public void add(Long seq) {
        Long current = this.seq;
        if (seq >= current + 1) {
            // add all possible exceptions
            for (Long e = current + 1; e <= seq - 1; e++) {
                this.exceptions.add(e);
            }

            // update seq
            this.seq = seq;
        } else {
            // remove from exceptions
            this.exceptions.remove(seq);
        }
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

    @Override
    public boolean equals(Object o) {
        // self check
        if (this == o) {
            return true;
        }
        // null check
        if (o == null) {
            return false;
        }
        // type check and cast
        if (getClass() != o.getClass()) {
            return false;
        }
        ExceptionSet t = (ExceptionSet) o;
        return Objects.equals(this.seq, t.seq) && this.exceptions.equals(t.exceptions);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.seq);
        if (!this.exceptions.isEmpty()) {
            sb.append(" ").append(this.exceptions);
        }
        return sb.toString();
    }

    @Override
    public Object clone() {
        ExceptionSet exceptionSet = new ExceptionSet(this.seq, (HashSet<Long>) this.exceptions.clone());
        return exceptionSet;
    }
}
