package org.imdea.vcd.queue;

import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.ExceptionSet;

/**
 *
 * @author Vitor Enes
 * @param <T>
 */
public interface DepBox<T> {

    boolean before(T box);

    void merge(T box);

    boolean canDeliver(Clock<ExceptionSet> delivered);
}
