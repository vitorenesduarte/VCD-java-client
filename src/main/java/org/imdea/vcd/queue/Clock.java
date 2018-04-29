package org.imdea.vcd.queue;

import java.util.HashSet;
import org.imdea.vcd.pb.Proto.Dot;

/**
 *
 * @author Vitor
 */
public abstract class Clock {

    public abstract boolean isElement(Dot dot);
    
    public boolean intersects(HashSet<Dot> dots) {
        for (Dot dot : dots) {
            if (this.isElement(dot)) {
                return true;
            }
        }

        return false;
    }
}
