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

    public static Integer MESSAGE_NUMBER = 100;
    public static List<String> MESSAGES = Arrays.asList("A", "B"); // 50% conflicts

    public static void main(String[] args) throws Exception {
        Thread.sleep(3000);

        VCDConfig config = VCDConfig.parseArgs(args);
        VCDSocket socket = VCDSocket.create(config);

        List<Long> latency = new ArrayList<>();

        for (int i = 0; i < MESSAGE_NUMBER; i++) {
            String message = nextMessage();

            Long start = System.nanoTime();
            socket.send(message);
            socket.receive();
            Long end = System.nanoTime();

            latency.add(end - start);
        }

        Long latencyAverage = average(latency);
        System.out.println("Latency: " + latencyAverage);
    }

    private static String nextMessage() {
        int index = ThreadLocalRandom.current().nextInt(MESSAGES.size());
        return MESSAGES.get(index);
    }

    private static Long average(List<Long> latency) {
        Long sum = 0L;
        for (Long l : latency) {
            sum += l;
        }
        return sum / latency.size();
    }
}
