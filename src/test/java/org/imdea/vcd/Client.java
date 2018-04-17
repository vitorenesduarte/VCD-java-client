package org.imdea.vcd;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
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

        private Socket socket;
        private ConcurrentHashMap<ByteString, PerData> ops;
        private LinkedBlockingQueue<Integer> queue;

        public Writer(Config config) {
            this.config = config;
            this.metrics = new Metrics();
            this.opsPerClient = new int[this.config.getClients()];
        }

        @Override
        public void run() {

            try {
                // connect to closest server and start reader thread
                this.init();
                LOGGER.log(Level.INFO, "Connect OK!");

                // start all clients
                this.startAllClients();

                for (int clientsDone = 0; clientsDone != config.getClients();) {
                    try {
                        int client = queue.take();

                        if (client == -1) {
                            this.init();
                        } else {
                            // increment ops by this client
                            this.opsPerClient[client]++;

                            // log every 100 ops
                            if (this.opsPerClient[client] % 100 == 0) {
                                LOGGER.log(Level.INFO, "{0} of {1}",
                                        new String[]{String.valueOf(opsPerClient[client]), String.valueOf(this.config.getOps())});
                            }

                            if (this.opsPerClient[client] == this.config.getOps()) {
                                // if it performed all the operations
                                // increment number of clients done
                                clientsDone++;
                            } else {
                                // otherwise send another operation
                                this.sendOp(client);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace(System.err);
                        // if at any point the socket errors inside this loop,
                        // reconnect to the closest server
                        // and start reader thread
                        this.init();
                        // and send a new op per client
                        this.startAllClients();
                    }
                }

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
            Reader reader = new Reader(this.config, this.metrics, this.ops, this.queue, this.socket);
            reader.start();
        }

        private void startAllClients() throws IOException {
            for (int i = 0; i < this.config.getClients(); i++) {
                this.sendOp(i);
            }
        }

        private void sendOp(int client) throws IOException {
            MessageSet messageSet = RandomMessageSet.generate(this.config);
            ByteString data = messageSet.getMessagesList().get(0).getData();
            if (this.ops.containsKey(data)) {
                // if this key already exists, try again
                this.sendOp(client);
            } else {
                // if it doesn't, send it and update map
                PerData perData = new PerData(client, this.metrics.start());
                this.ops.put(data, perData);
                this.socket.send(messageSet);
            }
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

        private static class Reader extends Thread {

            private final Config config;
            private final Metrics metrics;
            private final ConcurrentHashMap<ByteString, PerData> ops;
            private final LinkedBlockingQueue<Integer> queue;
            private final Socket socket;

            public Reader(Config config, Metrics metrics, ConcurrentHashMap<ByteString, PerData> ops, LinkedBlockingQueue<Integer> queue, Socket socket) {
                this.config = config;
                this.metrics = metrics;
                this.ops = ops;
                this.queue = queue;
                this.socket = socket;
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

                                            // notify writer thread, if closed-loop
                                            if (this.config.getClosedLoop()) {
                                                this.queue.put(client);
                                            }
                                        }
                                    }
                                    break;
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace(System.err);
                        // notify writer thread
                        this.queue.put(-1);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
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
    }
}
