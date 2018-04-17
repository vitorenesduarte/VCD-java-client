package org.imdea.vcd;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Vitor Enes
 */
public class Client {

    private static final Logger LOGGER = VCDLogger.init(Client.class);
    private static final int CONNECT_RETRIES = 100;

    public static void main(String[] args) {
        try {
            Config config = Config.parseArgs(args);
            Thread writer = new Writer(config);
            writer.start();
            writer.join();
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private static class Writer extends Thread {

        private final Config config;
        private final Metrics metrics;
        private final int[] opsPerClient;
        private final Semaphore done;

        private Socket socket;
        private ConcurrentHashMap<ByteString, PerData> ops;
        private LinkedBlockingQueue<Integer> queue;

        private int clientsDone;

        public Writer(Config config) {
            this.config = config;
            this.metrics = new Metrics();
            this.opsPerClient = new int[this.config.getClients()];
            this.done = new Semaphore(0);
            this.clientsDone = 0;
        }

        @Override
        public void run() {

            try {
                // connect to closest server and start reader thread
                this.init();
                LOGGER.log(Level.INFO, "Connect OK!");

                // start write loop
                this.writeLoop();

                // wait all last ops were delivered
                this.done.acquire();

                // after all operations from all clients, show metrics
                LOGGER.log(Level.INFO, this.metrics.show());

                // and push them to redis
                this.redisPush();

            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        }

        private void init() throws IOException, InterruptedException {
            this.ops = new ConcurrentHashMap<>();
            this.queue = new LinkedBlockingQueue<>();
            this.socket = Socket.create(this.config, CONNECT_RETRIES);
            Reader reader = new Reader(this.config, this.metrics, this.done, this.ops, this.queue, this.socket);
            reader.start();
        }

        private void writeLoop() throws IOException, InterruptedException {
            // start all clients
            this.nextOps();

            while (clientsDone != config.getClients()) {
                try {
                    if (this.config.getClosedLoop()) {
                        int client = queue.take();

                        if (client == -1) {
                            this.init();
                        } else {
                            // TODO make this class variable
                            this.nextOp(client);
                        }
                    } else {
                        Thread.sleep(this.config.getSleep());
                        this.nextOps();
                    }
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                    // if at any point the socket errors inside this loop,
                    // reconnect to the closest server
                    // and start reader thread
                    this.init();
                    // and send a new op per client
                    this.nextOps();
                }
            }
        }

        private void nextOps() throws IOException {
            for (int client = 0; client < this.config.getClients(); client++) {
                // check these are not the last ops
                this.nextOp(client);
            }
        }

        private void nextOp(int client) throws IOException {
            this.opsPerClient[client]++;

            if (this.opsPerClient[client] % 100 == 0) {
                LOGGER.log(Level.INFO, "{0} of {1}", new String[]{String.valueOf(opsPerClient[client]), String.valueOf(this.config.getOps())});
            }

            boolean lastOp = this.opsPerClient[client] == this.config.getOps();

            // generate data
            ByteString data;
            if (lastOp) {
                // if last op
                data = getLastOp(client);
                this.clientsDone++;
            } else {
                do {
                    data = MessageSetGen.generateData(this.config);
                } while (this.ops.containsKey(data));
            }

            // if it doesn't, send it and update map
            this.sendOp(client, data);
        }

        private void sendOp(int client, ByteString data) throws IOException {
            MessageSet messageSet = MessageSetGen.generate(this.config, data);
            PerData perData = new PerData(client, this.metrics.start());
            this.ops.put(data, perData);
            this.socket.send(messageSet);
        }

        private void redisPush() {
            String redis = this.config.getRedis();

            if (redis != null) {
                try (Jedis jedis = new Jedis(redis)) {
                    Map<String, String> push = this.metrics.serialize(this.config);
                    for (String key : push.keySet()) {
                        jedis.sadd(key, push.get(key));
                    }
                }
            }
        }
    }

    private static class Reader extends Thread {

        private final Config config;
        private final Metrics metrics;
        private final Semaphore done;
        private final ConcurrentHashMap<ByteString, PerData> ops;
        private final LinkedBlockingQueue<Integer> queue;
        private final Socket socket;

        private int lastOps;

        public Reader(Config config, Metrics metrics, Semaphore done, ConcurrentHashMap<ByteString, PerData> ops, LinkedBlockingQueue<Integer> queue, Socket socket) {
            this.config = config;
            this.metrics = metrics;
            this.done = done;
            this.ops = ops;
            this.queue = queue;
            this.socket = socket;
            this.lastOps = 0;
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
                            case COMMITTED:
                                data = messages.get(0).getData();
                                perData = this.ops.get(data);
                                // record commit time, if perData exists
                                // TODO check how to could have been delivered
                                // before being committed
                                // - maybe collision with another message,
                                //   in another node
                                // - or on recovery?
                                if (perData != null) {
                                    this.metrics.end(status, perData.getStartTime());
                                }
                                // keep waiting
                                break;
                            case DELIVERED:
                                // record chain size
                                this.metrics.chain(messages.size());

                                Iterator<Message> it = messages.iterator();

                                // try to find operations from clients
                                while (it.hasNext()) {
                                    data = it.next().getData();
                                    perData = this.ops.get(data);

                                    // if it belongs to a client
                                    if (perData != null) {
                                        int client = perData.getClient();
                                        Long startTime = perData.getStartTime();

                                        // delete from the map
                                        this.ops.remove(data);

                                        // record delivery time
                                        this.metrics.end(status, startTime);

                                        // notify writer thread
                                        this.maybeNotifyWriter(client);
                                        if (this.maybeNotifyLastOp(client, data)) {
                                            return;
                                        }
                                    }
                                }
                                break;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                    // notify writer thread
                    this.maybeNotifyWriter(-1);
                }
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }

        /**
         * If closed-loop, notify op from client was delivered.
         */
        private void maybeNotifyWriter(int client) throws InterruptedException {
            if (this.config.getClosedLoop()) {
                this.queue.put(client);
            }
        }

        /**
         * Release semaphore when all last ops were delivered.
         */
        private boolean maybeNotifyLastOp(int client, ByteString data) {
            if (isLastOp(client, data)) {
                this.lastOps++;
                if (this.lastOps == this.config.getClients()) {
                    this.done.release();
                    return true;
                }
            }

            return false;
        }
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

    private static ByteString getLastOp(int client) {
        ByteString data = ByteString.copyFromUtf8("last-op-" + client);
        return data;
    }

    private static boolean isLastOp(int client, ByteString data) {
        return data.toStringUtf8().equals("last-op-" + client);
    }
}
