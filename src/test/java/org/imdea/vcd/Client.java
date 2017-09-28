package org.imdea.vcd;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.imdea.vcd.datum.Proto.Message;
import org.imdea.vcd.datum.Proto.MessageSet;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Vitor Enes
 */
public class Client {

    public static void main(String[] args) throws Exception {
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
                    if (i % 100000 == 0) {
                        println(i + " of " + config.getOps());
                    }
                    MessageSet messageSet = RandomMessageSet.generate(config.getConflictPercentage(), 1);
                    ByteString id = messageSet.getMessagesList().get(0).getData();
                    Long start = this.timer.start();
                    socket.send(messageSet);
                    receiveMessage(start, id, socket);
                }

                println(this.timer.show());

                push(config);
            } catch (IOException ex) {
                Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        private void receiveMessage(Long start, ByteString id, Socket socket) throws IOException {
            boolean found = false;
            while (!found) {
                MessageSet messageSet = socket.receive();
                List<Message> messages = messageSet.getMessagesList();
                MessageSet.Status status = messageSet.getStatus();

                switch (status) {
                    case COMMITTED:
                        this.timer.end(status, start);
                        // keep waiting
                        break;
                    case DELIVERED:
                        Iterator<Message> it = messages.iterator();

                        // try to find the message I just sent
                        while (it.hasNext() && !found) {
                            found = it.next().getData().equals(id);
                        }

                        // if found, the cycle breaks
                        // otherwise, keep waiting
                        if (found) {
                            this.timer.end(status, start);
                        }
                        break;
                }
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