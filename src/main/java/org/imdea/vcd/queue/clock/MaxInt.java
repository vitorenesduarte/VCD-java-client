package org.imdea.vcd.queue.clock;

import java.util.Objects;

/**
 *
 * @author Vitor Enes
 */
public class MaxInt implements IntSet<MaxInt> {

    private Long seq;

    public MaxInt() {
        this.seq = 0L;
    }

    public MaxInt(Long seq) {
        this.seq = seq;
    }

    @Override
    public boolean contains(Long seq) {
        return seq <= this.seq;
    }

    @Override
    public void add(Long seq) {
        this.seq = Long.max(this.seq, seq);
    }

    @Override
    public void merge(MaxInt o) {
        this.seq = Long.max(this.seq, o.seq);
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
        MaxInt t = (MaxInt) o;
        return Objects.equals(this.seq, t.seq);
    }

    @Override
    public String toString() {
        return this.seq.toString();
    }
}
