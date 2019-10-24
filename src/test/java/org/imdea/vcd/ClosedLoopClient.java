package org.imdea.vcd;

import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class ClosedLoopClient {

    private static final Logger LOGGER = VCDLogger.init(ClosedLoopClient.class);
    private static final int CONNECT_RETRIES = 100;

    private static Config CONFIG;
    private static Socket SOCKET;
    private static Map<ByteString, PerData> MAP;
    private static int[] OPS_PER_CLIENT;
    private static ByteString[] CLIENT_KEY;
    private static int CLIENTS_DONE;

    public static void run(Config config) {
        try {
            CONFIG = config;
            LOGGER.log(Level.INFO, "Optimized delivery is {0}", CONFIG.getOptDelivery() ? "enabled" : "disabled");
            LOGGER.log(Level.INFO, "Payload size is {0}", CONFIG.getPayloadSize());
            LOGGER.log(Level.INFO, "Conflict rate is {0}", CONFIG.getConflicts());
            SOCKET = Socket.create(CONFIG, CONNECT_RETRIES);
            MAP = new HashMap<>();
            OPS_PER_CLIENT = new int[CONFIG.getClients()];
            CLIENT_KEY = new ByteString[CONFIG.getClients()];
            CLIENTS_DONE = 0;

            // create a unique key for each client
            for (int i = 0; i < CONFIG.getClients(); i++) {
                CLIENT_KEY[i] = Generator.randomClientKey();
            }

            LOGGER.log(Level.INFO, "Connect OK!");

            start();

            while (CLIENTS_DONE != CONFIG.getClients()) {
                try {
                    MessageSet messageSet = SOCKET.receive();
                    List<Message> messages = messageSet.getMessagesList();
                    MessageSet.Status status = messageSet.getStatus();

                    ByteString from;
                    PerData perData;
                    switch (status) {
                        case COMMIT:
                            for (Message message : messages) {
                                from = message.getFrom();
                                perData = MAP.get(from);
                                // record commit time, if perData exists
                                // TODO check how to could have been delivered
                                // before being committed
                                // - maybe collision with another message,
                                //   in another node
                                // - or on recovery?
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
                                from = message.getFrom();
                                perData = MAP.remove(from);

                                // if it belongs to a client
                                if (perData != null) {
                                    int client = perData.client;
                                    Long startTime = perData.startTime;

                                    // record delivery time
                                    ClientMetrics.end(status, startTime);
                                    perData.deliverContext.stop();

                                    // increment number of ops of this client
                                    OPS_PER_CLIENT[client]++;

                                    // log every 100 ops
                                    if (OPS_PER_CLIENT[client] % 100 == 0) {
                                        LOGGER.log(Level.INFO, "({0}) {1} of {2}", new String[]{
                                            String.valueOf(client),
                                            String.valueOf(OPS_PER_CLIENT[client]),
                                            String.valueOf(CONFIG.getOps())
                                        });
                                    }

                                    if (OPS_PER_CLIENT[client] == CONFIG.getOps()) {
                                        // if it performed all the operations
                                        // increment number of clients done
                                        CLIENTS_DONE++;
                                    } else {
                                        // otherwise send another operation
                                        sendOp(client);
                                    }
                                }
                            }
                            break;
                    }
                } catch (IOException e) {
                    // close current socket
                    SOCKET.close();
                    // if at any point the socket errors inside this loop,
                    // reconnect to the closest server
                    SOCKET = Socket.create(CONFIG, CONNECT_RETRIES);
                    // clear current map
                    MAP = new HashMap<>();
                    // and send a new op per client
                    start();
                }

            }

            // after all operations from all clients
            // show metrics
            LOGGER.log(Level.INFO, ClientMetrics.show());

            // and push them to redis
            redisPush();

            SOCKET.close();
        } catch (IOException | InterruptedException e) {
            LOGGER.log(Level.SEVERE, e.toString(), e);
        }
    }

    private static void start() throws IOException, InterruptedException {
        for (int i = 0; i < CONFIG.getClients(); i++) {
            sendOp(i);
        }
    }

    private static void sendOp(Integer client) throws IOException, InterruptedException {
        // generate message
        ByteString from = CLIENT_KEY[client];
        Message message = Generator.message(client, from, from, CONFIG);

        // store info
        PerData perData = new PerData(client, ClientMetrics.start());
        MAP.put(from, perData);

        // send op
        SOCKET.send(message);
    }

    private static void redisPush() {
        try {
            String redis = CONFIG.getRedis();

            if (redis != null) {
                try (Jedis jedis = new Jedis(redis)) {
                    Map<String, String> push = ClientMetrics.serialize(CONFIG);
                    for (String key : push.keySet()) {
                        jedis.sadd(key, push.get(key));
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.toString(), e);
            // if any exception, try again
            redisPush();
        }
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
