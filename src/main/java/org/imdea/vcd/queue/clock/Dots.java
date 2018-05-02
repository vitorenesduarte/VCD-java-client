package org.imdea.vcd.queue.clock;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

/**
 *
 * @author Vitor Enes
 */
public class Dots implements Iterable<Dot> {

    private final HashSet<Dot> set;

    public Dots(Dot dot) {
        this.set = new HashSet<>(Arrays.asList(dot));
    }

    public Dots(HashSet<Dot> set) {
        this.set = set;
    }

    public Dots(Dots dots) {
        this.set = new HashSet<>();
        for (Dot dot : dots.set) {
            this.set.add((Dot) dot.clone());
        }
    }

    public void merge(Dots dots) {
        this.set.addAll(dots.set);
    }

    public int size() {
        return this.set.size();
    }

    @Override
    public Iterator<Dot> iterator() {
        return this.set.iterator();
    }

    @Override
    public String toString() {
        return this.set.toString();
    }

    @Override
    public Object clone() {
        Dots dots = new Dots(this);
        return dots;
    }
}
