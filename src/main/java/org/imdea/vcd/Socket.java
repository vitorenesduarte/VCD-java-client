package org.imdea.vcd;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.imdea.vcd.datum.DatumCoder;
import org.imdea.vcd.datum.DatumType;

/**
 *
 * @author Vitor Enes
 */
public class Socket {

    private final DataRW rw;

    private Socket(DataRW rw) {
        this.rw = rw;
    }

    public static Socket create(Config config) throws IOException {
        java.net.Socket socket = new java.net.Socket(config.getHost(), config.getPort());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        socket.setTcpNoDelay(true);
        DataRW rw = new DataRW(in, out);
        return new Socket(rw);
    }

    public void send(DatumType type, Object record) throws IOException {
        this.rw.write(DatumCoder.encode(type, record));
    }

    public Object receive(DatumType type) throws IOException {
        return DatumCoder.decode(type, this.rw.read());
    }
}
