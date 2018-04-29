package org.imdea.vcd.queue;

import java.util.Arrays;
import java.util.HashSet;
import org.imdea.vcd.pb.Proto.Dot;
import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public abstract class DepBox {
    
    private final HashSet<Dot> dots;
    private final Clock dep;

    public DepBox(Dot dot, Clock dep) {
        this.dots = new HashSet<>(Arrays.asList(dot));
        this.dep = dep;
    }
    
    public Clock getDep() {
        return this.dep;
    }

    public HashSet<Dot> getDots() {
        return dots;
    }

    public boolean before(DepBox box) {
        return box.getDep().intersects(this.dots);
    }
    
    public abstract void merge(DepBox box);

    public abstract MessageSet toMessageSet();
}
