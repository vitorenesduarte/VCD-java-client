package org.imdea.vcd;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.MaxInt;

/**
 *
 * @author Vitor Enes
 */
public class Generator {

    private static final Integer KEY_SIZE = 8;
    private static final Integer MIN_ASCII = 33;
    private static final Integer MAX_ASCII = 126;
    private static final byte[] CHARACTERS = chars(MIN_ASCII, MAX_ASCII);

    private static final ByteString BLACK = repeat((byte) 1, 1);

    public static Message message() {
        Integer conflicts = RANDOM().nextInt(100);
        ByteString data = randomByteString(RANDOM().nextInt(100));
        return Message.newBuilder()
                .addHashes(hash(conflicts))
                .setData(data)
                .build();
    }

    public static MessageSet messageSet() {
        return messageSet(RANDOM().nextInt(100), randomByteString(RANDOM().nextInt(100)));
    }

    public static MessageSet messageSet(Config config) {
        return messageSet(config.getConflicts(), randomByteString(config.getPayloadSize()));
    }

    public static MessageSet messageSet(Integer conflicts, ByteString data) {
        MessageSet.Builder builder = MessageSet.newBuilder();

        Message m = Message.newBuilder()
                .addHashes(hash(conflicts))
                .setData(data)
                .build();
        builder.addMessages(m);

        builder.setStatus(MessageSet.Status.START);

        return builder.build();
    }

    public static ByteString messageSetData(Config config) {
        return randomByteString(config.getPayloadSize());
    }

    private static final int MAX_SEQ_PER_NODE = 10;
    private static final int MAX_DEPS = 5;

    public static Map<Dot, Clock<MaxInt>> dotToConf(Integer nodeNumber) {
        Map<Dot, Clock<MaxInt>> result = new HashMap<>();

        // create dots
        List<Dot> dots = new ArrayList<>();
        for (Integer id = 0; id < nodeNumber; id++) {
            Long maxSeq = RANDOM().nextLong(MAX_SEQ_PER_NODE);
            for (Long seq = 1L; seq <= maxSeq; seq++) {
                dots.add(new Dot(id, seq));
            }
        }

        List<Dot> deps = new ArrayList<>(dots);
        for (Dot dot : dots) {
            // for each dot, take a random subset of all dots as conf
            Collections.shuffle(deps);
            Integer numberOfDeps = RANDOM().nextInt(Math.min(MAX_DEPS, deps.size()));
            Clock<MaxInt> conf = new Clock<>(nodeNumber, new MaxInt());
            for (int i = 0; i < numberOfDeps; i++) {
                conf.addDot(deps.get(i));
            }
            conf.addDot(dot);
            result.put(dot, conf);
        }

        return result;
    }

    /**
     * Divide a range (0, m) into n unequal ranges
     */
    public static int[] ranges(int m, int n) {
        int[] ranges = new int[n];

        // if range is empty, return n empty ranges
        if (m == 0) {
            return ranges;
        }

        // if range max value is smaller or equal to n
        // return n (0, 1) ranges
        if (m <= n) {
            for (int i = 0; i < n; i++) {
                ranges[i] = 1;
            }

            return ranges;
        }

        // create unscaled ranges
        int[] unscaled = new int[n];
        int sum = 0;
        for (int i = 0; i < n; i++) {
            unscaled[i] = RANDOM().nextInt(m / 4, m);
            sum += unscaled[i];
        }

        // scale ranges
        for (int i = 0; i < n; i++) {
            ranges[i] = (unscaled[i] * m) / sum;
        }

        // and return them
        return ranges;
    }

    private static ThreadLocalRandom RANDOM() {
        return ThreadLocalRandom.current();
    }

    private static ByteString hash(Integer conflicts) {
        ByteString hash;
        if (RANDOM().nextInt(100) < conflicts) {
            hash = BLACK;
        } else {
            // try to avoid conflicts with random string
            hash = randomByteString(KEY_SIZE);
        }
        return hash;
    }

    private static ByteString repeat(byte b, Integer payloadSize) {
        byte[] ba = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            ba[i] = b;
        }
        return bs(ba);
    }

    private static ByteString randomByteString(Integer payloadSize) {
        byte[] ba = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            ba[i] = CHARACTERS[RANDOM().nextInt(CHARACTERS.length)];
        }
        return bs(ba);
    }

    private static ByteString bs(byte[] ba) {
        return ByteString.copyFrom(ba);
    }

    private static byte[] chars(Integer min, Integer max) {
        byte[] ba = new byte[max - min + 1];
        for (int i = min; i <= max; i++) {
            ba[i - min] = (byte) i;
        }
        return ba;
    }
}
