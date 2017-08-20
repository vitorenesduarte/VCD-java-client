package vcd;

import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Vitor Enes
 */
public class VCDClient {

    public static void main(String[] args) throws Exception {
        VCDConfig config = VCDConfig.parseArgs(args);
        VCDSocket socket = VCDSocket.create(config);

        List<String> messages = Arrays.asList("ola", ",", "tudo", "bem", "?");
        for (String message : messages) {
            socket.send(message);
            System.out.println("SENT:     " + message);
            System.out.println("RECEIVED: " + socket.receive());
        }
    }
}
