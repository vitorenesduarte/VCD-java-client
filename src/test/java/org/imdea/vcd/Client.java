package org.imdea.vcd;

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

            if (i % 250 == 0) {
                Debug.show();
            }

            MessageSet expected = RandomMessageSet.generate(config.getConflictPercentage(), 1);
            socket.send(expected);
            MessageSet result = socket.receive();

            assert expected.equals(result);
        }

        Thread.sleep(1000);
    }
}
