package org.imdea.vcd.queue;

import java.util.List;

/**
 *
 * @author Vitor Enes
 * @param <E>
 */
public interface DeliveryQueue<E extends DepBox> {

    void add(E e);

    int elements();

    boolean isEmpty();

    int size();

    List<E> toList();

    List<E> tryDeliver();
}
