package org.imdea.vcd.queue.clock;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import org.imdea.vcd.pb.Proto.Dot;

/**
 *
 * @author Vitor Enes
 */
public class Dots implements Iterable<Dot> {
    
    private final HashSet<Dot> set;

    public Dots(Dot dot) {
        this.set = new HashSet<>(Arrays.asList(dot));
    }
    
    public void merge(Dots dots) {
        this.set.addAll(dots.set);
    }

    @Override
    public Iterator<Dot> iterator() {
        return this.set.iterator();
    }
}
