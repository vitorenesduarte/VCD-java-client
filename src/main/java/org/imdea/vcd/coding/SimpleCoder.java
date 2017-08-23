package org.imdea.vcd.coding;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.imdea.vcd.MessageSet;

/**
 *
 * @author Vitor Enes
 *
 */
public class SimpleCoder implements Coder {

    public SimpleCoder() {
    }

    @Override
    public byte[] encode(MessageSet messageSet) throws IOException {
        ByteBuffer bb = messageSet.toByteBuffer();
        byte[] data = new byte[bb.remaining()];
        bb.get(data);
        return data;
    }

    @Override
    public MessageSet decode(byte[] data) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(data);
        return MessageSet.fromByteBuffer(bb);
    }
}
