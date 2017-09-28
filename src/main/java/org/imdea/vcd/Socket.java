package org.imdea.vcd;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.imdea.vcd.datum.MessageSetCoder;
import org.imdea.vcd.datum.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public class Socket {

    private final DataRW rw;
    private final MessageSetCoder coder;

    private Socket(DataRW rw) {
        this.rw = rw;
        this.coder = new MessageSetCoder();
    }

    public static Socket create(Config config) throws IOException {
        java.net.Socket socket = new java.net.Socket(config.getHost(), config.getPort());
        socket.setTcpNoDelay(true);

        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataRW rw = new DataRW(in, out);

        return new Socket(rw);
    }

    public void send( MessageSet messageSet) throws IOException {
        byte data[] = this.coder.encode(messageSet);
        this.rw.write(data);
    }

    public MessageSet receive() throws IOException {
        byte data[] = this.rw.read();
        MessageSet messageSet = this.coder.decode(data);
        return messageSet;
    }
}
