package org.imdea.vcd.queue.clock;

import java.util.List;

/**
 *
 * @author Vitor Enes
 * @param <T>
 */
public interface IntSet<T> {

    boolean contains(Long seq);

    void add(Long seq);

    void merge(T o);

    List<Long> subtract(T o);

    Object clone();
}
