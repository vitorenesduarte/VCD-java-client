package org.imdea.vcd.coding;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.imdea.vcd.Debug;
import org.imdea.vcd.MessageSet;

/**
 *
 * @author Vitor Enes
 *
 */
public class SimpleCoder implements Coder {

    private static final byte[] HEADER = {-61, 1, 51, -96, 122, -28, 12, -21, 99, 90};

    public SimpleCoder() {
    }

    @Override
    public byte[] encode(MessageSet messageSet) throws IOException {
        //Debug.start("Encode");
        ByteBuffer bb = messageSet.toByteBuffer();

        // don't send the header
        bb.position(HEADER.length);

        byte[] data = new byte[bb.remaining()];
        bb.get(data);
        //Debug.end("Encode");
        return data;
    }

    @Override
    public MessageSet decode(byte[] data) throws IOException {
        //Debug.start("Decode");
        ByteBuffer bb = ByteBuffer.allocate(HEADER.length + data.length);

        // prepend the header
        bb.put(HEADER);
        bb.put(data);
        bb.position(0);

        MessageSet messageSet = MessageSet.fromByteBuffer(bb);
        //Debug.end("Decode");
        return messageSet;
    }
}
