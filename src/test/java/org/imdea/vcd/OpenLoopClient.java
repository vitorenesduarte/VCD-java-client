package org.imdea.vcd;

import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.imdea.vcd.metrics.ClientMetrics;
import org.imdea.vcd.metrics.RWMetrics;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Vitor Enes
 */
public class OpenLoopClient {

    private static final Logger LOGGER = VCDLogger.init(ClosedLoopClient.class);
    private static final int CONNECT_RETRIES = 100;

    public static void run(Config config) {
        try {
            ClientWriter writer = new ClientWriter(config);
            writer.start();
            writer.join();
        } catch (InterruptedException e) {
            LOGGER.log(Level.SEVERE, e.toString(), e);
        }
    }

    private static class ClientWriter extends Thread {

        private final Config CONFIG;
        private Socket SOCKET;
        private ConcurrentHashMap<ByteString, PerData> MAP;
        private final int[] OPS_PER_CLIENT;
        private final ByteString[] CLIENT_KEY;
        private int CLIENTS_DONE;

        private ClientReader READER;
        private final Semaphore DONE;

        public ClientWriter(Config config) {
            this.CONFIG = config;
            this.OPS_PER_CLIENT = new int[config.getClients()];
            this.CLIENT_KEY = new ByteString[config.getClients()];
            // create a unique key for each client
            for (int i = 0; i < config.getClients(); i++) {
                this.CLIENT_KEY[i] = Generator.randomClientKey();
            }
            this.DONE = new Semaphore(0);
        }

        @Override
        public void run() {
            try {
                LOGGER.log(Level.INFO, "Optimized delivery is {0}", CONFIG.getOptDelivery() ? "enabled" : "disabled");
                LOGGER.log(Level.INFO, "Payload size is {0}", CONFIG.getPayloadSize());
                LOGGER.log(Level.INFO, "Conflict rate is {0}", CONFIG.getConflicts());

                init(false);
                LOGGER.log(Level.INFO, "Connect OK!");

                // start write loop
                writeLoop();

                // after all operations from all clients
                // show metrics
                DONE.acquire();
                LOGGER.log(Level.INFO, ClientMetrics.show());

                // and push them to redis
                redisPush();

                SOCKET.close();

            } catch (IOException | InterruptedException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }

        /**
         * Connect to closest server and start reader thread.
         */
        private void init(boolean sendOps) throws IOException, InterruptedException {
            if (READER != null) {
                READER.close();
                READER.interrupt();
            }
            // make all clients start from the same operation number
            // (the smallest one)
            int minOp = Integer.MAX_VALUE;
            for (int client = 0; client < CONFIG.getClients(); client++) {
                minOp = Math.min(minOp, OPS_PER_CLIENT[client]);
            }
            for (int client = 0; client < CONFIG.getClients(); client++) {
                OPS_PER_CLIENT[client] = minOp;
            }
            MAP = new ConcurrentHashMap<>();
            SOCKET = Socket.create(CONFIG, CONNECT_RETRIES);
            CLIENTS_DONE = 0;

            READER = new ClientReader(CONFIG, SOCKET, MAP, DONE);
            READER.start();

            // maybe send an op per client
            if (sendOps) {
                nextOps();
            }
        }

        private void writeLoop() throws IOException, InterruptedException {
            // start all clients
            nextOps();
            int nanoSleep = (int) Math.ceil(((float) CONFIG.getSleep() / CONFIG.getClients()) * 1000 * 1000);

            while (CLIENTS_DONE != CONFIG.getClients()) {
                try {
                    nextOps(nanoSleep);
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, e.toString(), e);
                    // if at any point the socket errors inside this loop,
                    // reconnect to the closest server
                    // and start reader thread
                    init(true);
                }
            }
        }

        private void nextOps() throws IOException, InterruptedException {
            nextOps(0);
        }

        private void nextOps(int nanoSleep) throws IOException, InterruptedException {
            for (int client = 0; client < CONFIG.getClients(); client++) {
                if (nanoSleep > 0) {
                    LockSupport.parkNanos(nanoSleep);
                }
                nextOp(client);
            }
        }

        private void nextOp(int client) throws IOException, InterruptedException {
            OPS_PER_CLIENT[client]++;

            if (OPS_PER_CLIENT[client] % 100 == 0) {
                LOGGER.log(Level.INFO, "({0}) {1} of {2}", new String[]{
                    String.valueOf(client),
                    String.valueOf(OPS_PER_CLIENT[client]),
                    String.valueOf(CONFIG.getOps())
                });
            }

            // generate data
            ByteString data;

            if (OPS_PER_CLIENT[client] < CONFIG.getOps()) {
                // normal op
                do {
                    data = Generator.messageData(CONFIG);
                } while (MAP.containsKey(data));

            } else if (OPS_PER_CLIENT[client] == CONFIG.getOps()) {
                // last op
                data = getLastOp(CONFIG, client);
                CLIENTS_DONE++;

            } else {
                // client is done
                return;
            }

            sendOp(client, data);
        }

        private void sendOp(Integer client, ByteString data) throws IOException, InterruptedException {
            // generate message
            ByteString from = CLIENT_KEY[client];
            Message message = Generator.message(client, from, from, data, CONFIG);

            // store info
            PerData perData = new PerData(client, ClientMetrics.start());
            MAP.put(data, perData);

            // send op
            SOCKET.send(message);
        }

        private void redisPush() {
            String redis = CONFIG.getRedis();
            if (redis != null) {
                try (Jedis jedis = new Jedis(redis)) {
                    Map<String, String> push = ClientMetrics.serialize(CONFIG);
                    for (String key : push.keySet()) {
                        jedis.sadd(key, push.get(key));
                    }
                }
            }
        }

    }

    private static class ClientReader extends Thread {

        // shared with writer thread
        private final Config CONFIG;
        private final Socket SOCKET;
        private final ConcurrentHashMap<ByteString, PerData> MAP;
        private final Semaphore DONE;

        // local
        private int CLIENTS_DONE;

        /**
         * Since we're only releasing the done semaphore once we see the last op
         * from all clients, this implementation assumes that by the time this
         * thread is created, no last op from any client was delivered.
         */
        public ClientReader(Config config, Socket socket, ConcurrentHashMap<ByteString, PerData> ops, Semaphore done) {
            this.CONFIG = config;
            this.SOCKET = socket;
            this.MAP = ops;
            this.DONE = done;
            this.CLIENTS_DONE = 0;
        }

        public void close() throws IOException {
            SOCKET.close();
        }

        @Override
        public void run() {
            try {
                try {
                    while (true) {
                        MessageSet messageSet = SOCKET.receive();
                        List<Message> messages = messageSet.getMessagesList();
                        MessageSet.Status status = messageSet.getStatus();

                        ByteString data;
                        PerData perData;
                        switch (status) {
                            case COMMIT:
                                for (Message message : messages) {
                                    data = message.getData();
                                    perData = MAP.get(data);
                                    // record commit time, if perData exists
                                    if (perData != null) {
                                        ClientMetrics.end(status, perData.startTime);
                                        perData.commitContext.stop();
                                    }
                                }
                                // keep waiting
                                break;
                            case DELIVERED:
                                // record chain size
                                ClientMetrics.chain(messages.size());

                                // try to find operations from clients
                                for (Message message : messages) {
                                    data = message.getData();
                                    perData = MAP.remove(data);

                                    // if it belongs to a client
                                    if (perData != null) {
                                        int client = perData.client;
                                        Long startTime = perData.startTime;

                                        // record delivery time
                                        ClientMetrics.end(status, startTime);
                                        perData.deliverContext.stop();

                                        // if last op, notify writer thread, and exit
                                        if (maybeNotifyLastOp(client, data)) {
                                            return;
                                        }
                                    }
                                }
                                break;
                        }
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, e.toString(), e);
                }
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }

        /**
         * Check if it was the last operation from this client, and if all
         * clients are done, notify the writer thread by releasing the done
         * semaphore.
         */
        private boolean maybeNotifyLastOp(int client, ByteString data) throws InterruptedException {
            if (isLastOp(CONFIG, client, data)) {
                CLIENTS_DONE++;
            }

            boolean allClientsAreDone = CLIENTS_DONE == CONFIG.getClients();
            if (allClientsAreDone) {
                DONE.release();
            }
            return allClientsAreDone;
        }
    }

    private static ByteString getLastOp(Config config, int client) {
        ByteString data = ByteString.copyFromUtf8(lastOp(config, client));
        return data;
    }

    private static boolean isLastOp(Config config, int client, ByteString data) {
        return data.toStringUtf8().equals(lastOp(config, client));
    }

    private static String lastOp(Config config, int client) {
        return config.getCluster() + "-last-op-" + client;
    }

    private static class PerData {

        private final int client;
        private final Long startTime;
        private final Timer.Context commitContext;
        private final Timer.Context deliverContext;

        public PerData(int client, Long startTime) {
            this.client = client;
            this.startTime = startTime;
            this.commitContext = RWMetrics.createCommitContext();
            this.deliverContext = RWMetrics.createDeliverContext();
        }
    }
}
