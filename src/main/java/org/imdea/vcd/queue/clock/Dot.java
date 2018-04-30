package org.imdea.vcd.queue.clock;

import java.util.Objects;
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

    public Dot(Dot dot) {
        this.id = dot.id;
        this.seq = dot.seq;
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

    @Override
    public Object clone() {
        Dot dot = new Dot(this);
        return dot;
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
        Dot t = (Dot) o;
        return Objects.equals(this.id, t.id) && Objects.equals(this.seq, t.seq);
    }
}
