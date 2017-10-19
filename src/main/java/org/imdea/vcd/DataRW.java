package org.imdea.vcd;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.imdea.vcd.pb.Proto.MessageSet;

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
        in.readFully(data, 0, length);
        MessageSet messageSet = MessageSet.parseFrom(data);
        return messageSet;
    }

    public void close() throws IOException {
        this.in.close();
        this.out.close();
    }
}
