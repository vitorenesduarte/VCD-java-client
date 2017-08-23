package org.imdea.vcd.coding;

import java.io.IOException;
import org.imdea.vcd.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public interface Coder {

    MessageSet decode(byte[] data) throws IOException;

    byte[] encode(MessageSet t) throws IOException;

}
