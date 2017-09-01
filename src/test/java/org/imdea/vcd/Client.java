package org.imdea.vcd;

import static org.imdea.vcd.datum.DatumType.MESSAGE_SET;
import org.imdea.vcd.datum.MessageSet;

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
            MessageSet expected = RandomMessageSet.generate(config.getConflictPercentage(), 1);
            Debug.start("LATENCY");
            socket.send(MESSAGE_SET, expected);
            MessageSet result = (MessageSet) socket.receive(MESSAGE_SET);
            Debug.end("LATENCY");

            assert expected.equals(result);
        }

        Debug.show();

        Thread.sleep(1000);
    }
}
