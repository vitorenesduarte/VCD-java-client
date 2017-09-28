package org.imdea.vcd;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.imdea.vcd.datum.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public class DataRW {

    private final DataInputStream in;
    private final DataOutputStream out;

    public DataRW(DataInputStream in, DataOutputStream out) {
        this.in = in;
        this.out = out;
    }

    public void write(MessageSet messageSet) throws IOException {
        byte[] data = messageSet.toByteArray();
        out.writeInt(data.length);
        out.write(data, 0, data.length);
        out.flush();
    }

    public MessageSet read() throws IOException {
        int length = in.readInt();
        byte data[] = new byte[length];
        in.read(data, 0, length);
        MessageSet messageSet = MessageSet.parseFrom(data);
        return messageSet;
    }
}
