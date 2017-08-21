package vcd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author Vitor Enes
 */
public class VCDClient {

    public static void main(String[] args) throws Exception {
        Thread.sleep(3000);

        VCDConfig config = VCDConfig.parseArgs(args);
        VCDSocket socket = VCDSocket.create(config);

        List<Long> latency = new ArrayList<>();

        for (int i = 0; i < config.getOps(); i++) {
            String op = nextMessage(config.getConflictPercentage());

            Long start = System.nanoTime();
            socket.send(op);
            socket.receive();
            Long end = System.nanoTime();

            latency.add(end - start);
        }

        Long latencyAverageNano = average(latency);
        System.out.println("Latency (ms): " + toMilli(latencyAverageNano));
    }

    private static String nextMessage(Integer conflictPercentage) {
        Long result = System.nanoTime();

        if (conflictPercentage > 0) {
            Integer numberOfOps = 100 / conflictPercentage;
            result = ThreadLocalRandom.current().nextLong(numberOfOps);
        }

        return "" + result;
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
