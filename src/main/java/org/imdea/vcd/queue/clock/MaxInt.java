package org.imdea.vcd.queue.clock;

/**
 *
 * @author Vitor Enes
 */
public class MaxInt implements IntSet<MaxInt> {

    private Long seq;

    public MaxInt(Long seq) {
        this.seq = seq;
    }

    @Override
    public boolean contains(Long seq) {
        return seq <= this.seq;
    }

    @Override
    public void merge(MaxInt o) {
        MaxInt merged = merge(this, o);
        this.seq = merged.seq;
    }

    private MaxInt merge(MaxInt a, MaxInt b) {
        Long newSeq = Long.max(a.seq, b.seq);
        MaxInt c = new MaxInt(newSeq);
        return c;
    }
}
