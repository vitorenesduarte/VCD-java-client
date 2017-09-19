package org.imdea.vcd;

import java.io.IOException;
import org.imdea.vcd.datum.DatumType;
import org.imdea.vcd.datum.MessageSet;
import org.imdea.vcd.datum.Status;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Vitor Enes
 */
public class Client {

    public static void main(String[] args) throws Exception {
        Thread.sleep(10 * 1000);

        Config config = Config.parseArgs(args);
        Socket socket = Socket.create(config);
        
        System.out.println("Connect OK!");

        for (int i = 1; i <= config.getOps(); i++) {
            if (i % 1000 == 0) {
                System.out.println(i + " of " + config.getOps());
            }
            MessageSet messageSet = RandomMessageSet.generate(config.getConflictPercentage(), 1);
            Long start = Timer.start();
            socket.send(DatumType.MESSAGE_SET, messageSet);
            receiveStatus(socket, start);
        }

        Timer.show();

        push(config);

        Thread.sleep(1000);
    }

    private static void receiveStatus(Socket socket, Long start) throws IOException {
        Status status = (Status) socket.receive(DatumType.STATUS);

        Timer.end(status, start);
        if (status != Status.DELIVERED) {
            // block until delivered status is received
            receiveStatus(socket, start);
        }
    }

    private static void push(Config config) {
        String redis = config.getRedis();
        String timestamp = config.getTimestamp();

        if (redis != null) {
            try (Jedis jedis = new Jedis(redis)) {
                jedis.sadd(timestamp, Timer.serialize());
            }
        }
    }
}
