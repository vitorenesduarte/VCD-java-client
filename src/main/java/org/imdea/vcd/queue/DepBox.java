package org.imdea.vcd.queue;

import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 * @param <T>
 */
public interface DepBox<T> {

    boolean before(T box);

    void merge(T box);

    MessageSet toMessageSet();
}
