package org.imdea.vcd;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 *
 * @author Vitor Enses
 */
public class DataRW {

    private final DataInputStream in;
    private final DataOutputStream out;

    public DataRW(DataInputStream in, DataOutputStream out) {
        this.in = in;
        this.out = out;
    }

    public void write(byte[] data) throws IOException {
        out.writeInt(data.length);
        out.write(data, 0, data.length);
        out.flush();
    }

    public byte[] read() throws IOException {
        int length = in.readInt();
        byte data[] = new byte[length];
        in.read(data, 0, length);
        return data;
    }
}
