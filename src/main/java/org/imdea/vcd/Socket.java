package org.imdea.vcd;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author Vitor Enes
 */
public class Socket {

    private final DataInputStream in;
    private final DataOutputStream out;

    private Socket(InputStream in, OutputStream out) {
        this.in = new DataInputStream(in);
        this.out = new DataOutputStream(out);
    }

    public static Socket create(Config config) throws IOException {
        java.net.Socket socket = new java.net.Socket(config.getHost(), config.getPort());
        return new Socket(socket.getInputStream(), socket.getOutputStream());
    }

    public void send(MessageSet messageSet) throws IOException {
        write(messageSet.toByteBuffer().array());
    }
    
    public MessageSet receive() throws IOException {
        byte[] data = read();
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length);
        return MessageSet.fromByteBuffer(byteBuffer);
    }

    private void write(byte[] data) throws IOException {
        out.writeInt(data.length);
        out.write(data, 0, data.length);
        out.flush();
    }

    private byte[] read() throws IOException {
        int length = in.readInt();
        byte data[] = new byte[length];
        in.read(data, 0, length);
        return data;
    }
}
