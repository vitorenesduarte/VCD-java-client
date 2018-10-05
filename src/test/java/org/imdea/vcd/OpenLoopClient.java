package org.imdea.vcd;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Vitor Enes
 */
public class OpenLoopClient {

    private static final Logger LOGGER = VCDLogger.init(Client.class);
    private static final int CONNECT_RETRIES = 100;

    public static void main(String[] args) {
        try {
            Config config = Config.parseArgs(args);
            ClientWriter writer = new ClientWriter(config);
            writer.start();
            writer.join();
        } catch (InterruptedException e) {
            LOGGER.log(Level.SEVERE, e.toString(), e);
        }
    }

    private static class ClientWriter extends Thread {

        private final Config config;
        private final int[] opsPerClient;
        private final ByteString[] clientsKey;
        private final Semaphore done;

        private ConcurrentHashMap<ByteString, PerData> opToData;
        private LinkedBlockingQueue<Integer> toWriter;
        private Socket socket;
        private int clientsDone;
        private ClientReader reader;

        public ClientWriter(Config config) {
            this.config = config;
            this.opsPerClient = new int[this.config.getClients()];
            this.clientsKey = new ByteString[this.config.getClients()];
            // create a unique key for each client
            for (int i = 0; i < this.config.getClients(); i++) {
                this.clientsKey[i] = Generator.randomClientKey();
            }
            this.done = new Semaphore(0);
        }

        @Override
        public void run() {
            try {
                this.init(false);
                LOGGER.log(Level.INFO, "Connect OK!");

                // start write loop
                this.writeLoop();

                // after all operations from all clients
                // show metrics
                this.done.acquire();
                LOGGER.log(Level.INFO, Metrics.show());

                // and push them to redis
                this.redisPush();

                this.socket.close();

            } catch (IOException | InterruptedException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }

        /**
         * Connect to closest server and start reader thread.
         */
        private void init(boolean sendOps) throws IOException, InterruptedException {
            if (this.reader != null) {
                this.reader.close();
                this.reader.interrupt();
            }
            // make all clients start from the same operation number
            // (the smallest one)
            int minOp = Integer.MAX_VALUE;
            for (int client = 0; client < this.config.getClients(); client++) {
                minOp = Math.min(minOp, this.opsPerClient[client]);
            }
            for (int client = 0; client < this.config.getClients(); client++) {
                this.opsPerClient[client] = minOp;
            }
            this.opToData = new ConcurrentHashMap<>();
            this.toWriter = new LinkedBlockingQueue<>();
            this.socket = Socket.create(this.config, CONNECT_RETRIES);
            this.clientsDone = 0;
            this.reader = new ClientReader(this.config, this.socket, this.opToData, this.done, this.toWriter);
            reader.start();

            // maybe send an op per client
            if (sendOps) {
                this.nextOps();
            }
        }

        private void writeLoop() throws IOException, InterruptedException {
            // start all clients
            this.nextOps();
            int nanoSleep = (int) Math.ceil(((float) this.config.getSleep() / this.config.getClients()) * 1000 * 1000);

            while (this.clientsDone != this.config.getClients()) {
                try {
                    if (this.config.getClosedLoop()) {
                        int client = this.toWriter.take();

                        if (client == -1) {
                            // there was an error reading from the socket
                            this.init(true);
                        } else {
                            this.nextOp(client);
                        }
                    } else {
                        this.nextOps(nanoSleep);
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, e.toString(), e);
                    // if at any point the socket errors inside this loop,
                    // reconnect to the closest server
                    // and start reader thread
                    this.init(true);
                }
            }
        }

        private void nextOps() throws IOException, InterruptedException {
            nextOps(0);
        }

        private void nextOps(int nanoSleep) throws IOException, InterruptedException {
            for (int client = 0; client < this.config.getClients(); client++) {
                if (nanoSleep > 0) {
                    LockSupport.parkNanos(nanoSleep);
                }
                this.nextOp(client);
            }
        }

        private void nextOp(int client) throws IOException, InterruptedException {
            this.opsPerClient[client]++;

            if (this.opsPerClient[client] % 100 == 0) {
                LOGGER.log(Level.INFO, "({0}) {1} of {2}", new String[]{
                    String.valueOf(client),
                    String.valueOf(opsPerClient[client]),
                    String.valueOf(this.config.getOps())
                });
            }

            // generate data
            ByteString data;

            if (this.opsPerClient[client] < this.config.getOps()) {
                // normal op
                do {
                    data = Generator.messageSetData(this.config);
                } while (this.opToData.containsKey(data));

            } else if (this.opsPerClient[client] == this.config.getOps()) {
                // last op
                data = getLastOp(this.config, client);
                this.clientsDone++;

            } else {
                // client is done
                return;
            }

            this.sendOp(client, data);
        }

        private void sendOp(int client, ByteString data) throws IOException, InterruptedException {
            MessageSet messageSet = Generator.messageSet(this.clientsKey[client], this.config.getConflicts(), data);
            PerData perData = new PerData(client, Metrics.start());
            this.opToData.put(data, perData);
            this.socket.send(messageSet);
        }

        private void redisPush() {
            String redis = this.config.getRedis();
            if (redis != null) {
                try (Jedis jedis = new Jedis(redis)) {
                    Map<String, String> push = Metrics.serialize(this.config);
                    for (String key : push.keySet()) {
                        jedis.sadd(key, push.get(key));
                    }
                }
            }
        }

    }

    private static class ClientReader extends Thread {

        // shared with writer thread
        private final Config config;
        private final Socket socket;
        private final ConcurrentHashMap<ByteString, PerData> opToData;
        private final Semaphore done;
        private final LinkedBlockingQueue<Integer> toWriter;

        // local
        private int clientsDone;

        /**
         * Since we're only releasing the done semaphore once we see the last op
         * from all clients, this implementation assumes that by the time this
         * thread is created, no last op from any client was delivered.
         */
        public ClientReader(Config config, Socket socket, ConcurrentHashMap<ByteString, PerData> ops, Semaphore done, LinkedBlockingQueue<Integer> toWriter) {
            this.config = config;
            this.socket = socket;
            this.opToData = ops;
            this.done = done;
            this.toWriter = toWriter;
            this.clientsDone = 0;
        }

        public void close() throws IOException {
            this.socket.close();
        }

        @Override
        public void run() {
            try {
                try {
                    while (true) {
                        MessageSet messageSet = this.socket.receive();
                        List<Message> messages = messageSet.getMessagesList();
                        MessageSet.Status status = messageSet.getStatus();

                        ByteString data;
                        PerData perData;
                        switch (status) {
                            case DURABLE:
                                data = messages.get(0).getData();
                                perData = this.opToData.get(data);
                                // record commit time, if perData exists
                                if (perData != null) {
                                    Metrics.end(status, perData.getStartTime());
                                }
                                // keep waiting
                                break;
                            case DELIVERED:
                                // record chain size
                                Metrics.chain(messages.size());

                                Iterator<Message> it = messages.iterator();

                                // try to find operations from clients
                                while (it.hasNext()) {
                                    data = it.next().getData();
                                    perData = this.opToData.remove(data);

                                    // if it belongs to a client
                                    if (perData != null) {
                                        int client = perData.getClient();
                                        Long startTime = perData.getStartTime();

                                        // record delivery time
                                        Metrics.end(status, startTime);

                                        // notify writer thread
                                        this.maybeNotifyOp(client);

                                        // if last op, notify writer thread, and exit
                                        if (this.maybeNotifyLastOp(client, data)) {
                                            return;
                                        }
                                    }
                                }
                                break;
                        }
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, e.toString(), e);
                    this.maybeNotifyOp(-1);
                }
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, e.toString(), e);
            }
        }

        /**
         * If close loop, notify writer thread of which client just delivered an
         * operation.
         */
        private void maybeNotifyOp(int client) throws InterruptedException {
            if (this.config.getClosedLoop()) {
                this.toWriter.put(client);
            }
        }

        /**
         * Check if it was the last operation from this client, and if all
         * clients are done, notify the writer thread by releasing the done
         * semaphore.
         */
        private boolean maybeNotifyLastOp(int client, ByteString data) throws InterruptedException {
            if (isLastOp(this.config, client, data)) {
                this.clientsDone++;
            }

            boolean allClientsAreDone = this.clientsDone == this.config.getClients();
            if (allClientsAreDone) {
                this.done.release();
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

        public PerData(int client, Long startTime) {
            this.client = client;
            this.startTime = startTime;
        }

        public int getClient() {
            return client;
        }

        public long getStartTime() {
            return startTime;
        }
    }
}
