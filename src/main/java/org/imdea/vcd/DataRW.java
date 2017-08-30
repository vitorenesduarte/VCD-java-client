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
        //Debug.start("SocketWrite");
        out.writeInt(data.length);
        out.write(data, 0, data.length);
        out.flush();
        //Debug.end("SocketWrite");
    }

    public byte[] read() throws IOException {
        //Debug.start("SocketRead");
        int length = in.readInt();
        byte data[] = new byte[length];
        in.read(data, 0, length);
        //Debug.end("SocketRead");
        return data;
    }
}
