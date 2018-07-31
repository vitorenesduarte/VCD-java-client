package org.imdea.vcd.queue;

import org.imdea.vcd.queue.box.QueueBox;
import java.util.List;
import org.imdea.vcd.pb.Proto.Commit;

/**
 *
 * @author Vitor Enes
 * @param <E>
 */
public interface Queue<E extends QueueBox> {

    boolean isEmpty();

    void add(E e, Commit commit);

    List<E> tryDeliver();

    List<E> toList();

    int size();

    int elements();

}
