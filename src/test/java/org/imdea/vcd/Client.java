package org.imdea.vcd;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        Integer clients = config.getClients();
        ClientRunner runners[] = new ClientRunner[clients];

        for (int i = 0; i < clients; i++) {
            ClientRunner runner = new ClientRunner(config);
            runners[i] = runner;
        }

        for (int i = 0; i < clients; i++) {
            runners[i].start();
        }

        for (int i = 0; i < clients; i++) {
            runners[i].join();
        }
    }

    public static void println(String s) {
        synchronized (System.out) {
            System.out.println(s);
        }
    }

    private static class ClientRunner extends Thread {

        private final Config config;
        private final Timer timer;

        public ClientRunner(Config config) {
            this.config = config;
            this.timer = new Timer();
        }

        @Override
        public void run() {
            try {
                Socket socket = Socket.create(config);

                println("Connect OK!");

                for (int i = 1; i <= config.getOps(); i++) {
                    if (i % 100 == 0) {
                        println(i + " of " + config.getOps());
                    }
                    MessageSet messageSet = RandomMessageSet.generate(config.getConflictPercentage(), 1);
                    Long start = this.timer.start();
                    socket.send(DatumType.MESSAGE_SET, messageSet);
                    receiveStatus(socket, start);
                }

                println(this.timer.show());

                push(config);
            } catch (IOException ex) {
                Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        private void receiveStatus(Socket socket, Long start) throws IOException {
            Status status = (Status) socket.receive(DatumType.STATUS);

            this.timer.end(status, start);
            if (status != Status.DELIVERED) {
                // block until delivered status is received
                receiveStatus(socket, start);
            }
        }

        private void push(Config config) {
            String redis = config.getRedis();
            String timestamp = config.getTimestamp();

            if (redis != null) {
                try (Jedis jedis = new Jedis(redis)) {
                    jedis.sadd(timestamp, this.timer.serialize());
                }
            }
        }
    }
}
