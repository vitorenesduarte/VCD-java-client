package org.imdea.vcd;

import java.io.IOException;
import org.imdea.vcd.datum.DatumType;
import org.imdea.vcd.datum.MessageSet;
import org.imdea.vcd.datum.Status;

/**
 *
 * @author Vitor Enes
 */
public class Client {

    public static void main(String[] args) throws Exception {
        Thread.sleep(1000);

        Config config = Config.parseArgs(args);
        Socket socket = Socket.create(config);

        for (int i = 1; i <= config.getOps(); i++) {
            MessageSet messageSet = RandomMessageSet.generate(config.getConflictPercentage(), 1);
            Long start = Debug.start();
            socket.send(DatumType.MESSAGE_SET, messageSet);

            receiveStatus(socket, start);
        }

        Debug.show();

        Thread.sleep(1000);
    }

    private static void receiveStatus(Socket socket, Long start) throws IOException {
        Status result = (Status) socket.receive(DatumType.STATUS);

        switch (result) {
            case WRITTEN:
                Debug.end("WRITTEN", start);
                receiveStatus(socket, start);
                break;
            case COMMITTED:
                Debug.end("COMMITTED", start);
                receiveStatus(socket, start);
                break;
            case DELIVERED:
                Debug.end("DELIVERED", start);
                // block until this status is received
                break;
        }
    }
}
