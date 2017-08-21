package vcd;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 *
 * @author Vitor Enes
 */
public class VCDSocket {

    private final DataInputStream in;
    private final DataOutputStream out;

    private VCDSocket(InputStream in, OutputStream out) {
        this.in = new DataInputStream(in);
        this.out = new DataOutputStream(out);
    }

    public static VCDSocket create(VCDConfig config) throws IOException {
        Socket socket = new Socket(config.getHost(), config.getPort());
        return new VCDSocket(socket.getInputStream(), socket.getOutputStream());
    }

    public void send(String data) throws IOException {
        write(data.getBytes());
    }
    
    public String receive() throws IOException {
        return new String(read());
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
