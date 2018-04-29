package org.imdea.vcd.queue.clock;

/**
 *
 * @author Vitor Enes
 * @param <T>
 */
public interface IntSet<T> {

    boolean contains(Long seq);

    void merge(T o);
}
