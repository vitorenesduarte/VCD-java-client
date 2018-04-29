package org.imdea.vcd.queue;

import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public abstract class DepBox {
    public abstract boolean after(DepBox box);
    public abstract boolean before(DepBox box);
    public abstract void merge(DepBox box);
    public abstract MessageSet toMessageSet();
}
