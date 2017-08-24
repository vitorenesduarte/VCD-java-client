package org.imdea.vcd;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author Vitor Enes
 */
public class Client {

    public static void main(String[] args) throws Exception {
        Thread.sleep(3000);

        Config config = Config.parseArgs(args);
        Socket socket = Socket.create(config);

        List<Long> latency = new ArrayList<>();

        for (int i = 0; i < config.getOps(); i++) {
            Message m = nextMessage(config.getConflictPercentage());
            MessageSet messageSet = new MessageSet(Arrays.asList(m));

            Long start = System.nanoTime();
            socket.send(messageSet);
            socket.receive();
            Long end = System.nanoTime();

            latency.add(end - start);
        }

        Long latencyAverageNano = average(latency);
        System.out.println("LATENCY: " + toMilli(latencyAverageNano));
    }

    private static Message nextMessage(Integer conflictPercentage) {
        Long result = System.nanoTime();

        if (conflictPercentage > 0) {
            Integer numberOfOps = 100 / conflictPercentage;
            result = ThreadLocalRandom.current().nextLong(numberOfOps);
        }

        String hash = "" + result;
        String data = UUID.randomUUID().toString();
        Message message = new Message(ByteBuffer.wrap(hash.getBytes()), ByteBuffer.wrap(data.getBytes()));
        return message;
    }

    private static Long toMilli(Long nano) {
        return nano / 1000000;
    }

    private static Long average(List<Long> latency) {
        Long sum = 0L;
        for (Long l : latency) {
            sum += l;
        }
        return sum / latency.size();
    }
}
