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
        Debug.start("DataRW.write");
        out.writeInt(data.length);
        out.write(data, 0, data.length);
        out.flush();
        Debug.end("DataRW.write");
    }

    public byte[] read() throws IOException {
        Debug.start("DataRW.readInt");
        int length = in.readInt();
        Debug.end("DataRW.readInt");
        byte data[] = new byte[length];
        Debug.start("DataRW.read");
        in.read(data, 0, length);
        Debug.end("DataRW.read");
        return data;
    }
}
