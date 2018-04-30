package org.imdea.vcd.queue.clock;

import org.imdea.vcd.pb.Proto;

/**
 *
 * @author Vitor Enes
 */
public class Dot {

    private final Integer id;
    private final Long seq;

    public Dot(Integer id, Long seq) {
        this.id = id;
        this.seq = seq;
    }

    public Integer getId() {
        return id;
    }

    public Long getSeq() {
        return seq;
    }

    @Override
    public String toString() {
        return "{" + id + "," + seq + '}';
    }

    public static Dot dot(Proto.Dot dot) {
        return new Dot(dot.getId(), dot.getSeq());
    }
}
