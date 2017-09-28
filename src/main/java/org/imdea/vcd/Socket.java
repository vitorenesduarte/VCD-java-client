package org.imdea.vcd;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.imdea.vcd.datum.Proto.MessageSet;

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

    public void send(MessageSet messageSet) throws IOException {
        this.rw.write(messageSet);
    }

    public MessageSet receive() throws IOException {
        return this.rw.read();
    }
}
