package org.imdea.vcd;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.queue.clock.Clock;
import org.imdea.vcd.queue.clock.Dot;
import org.imdea.vcd.queue.clock.MaxInt;

/**
 *
 * @author Vitor Enes
 */
public class Generator {

    private static final Integer KEY_SIZE = 8;
    public static final ByteString BLACK = repeat((byte) 1, 1);

    public static final Integer ZIPF_ELEMENTS = 10 * 1000;
    public static ZipfDistribution ZIPF;

    public static Message message() {
        return message(randomByteString(KEY_SIZE));
    }

    public static Message message(String key) {
        return message(bs(key.getBytes()));
    }

    public static Message message(ByteString key) {
        ByteString data = randomByteString(RANDOM().nextInt(100));
        Message m = Message.newBuilder()
                .addHashes(key)
                .setData(data)
                .setPure(false)
                .build();
        return m;
    }

    // the following two methods are the ones used by the clients
    public static Message message(Integer client, ByteString key, ByteString from, Config config) {
        return message(client, from, from, randomByteString(config.getPayloadSize()), config);
    }

    public static Message message(Integer client, ByteString key, ByteString from, ByteString data, Config config) {
        Message m = Message.newBuilder()
                .addHashes(hash(client, key, config))
                .setData(data)
                .setPure(false)
                .setFrom(from)
                .build();
        return m;
    }

    public static ByteString messageData(Config config) {
        return randomByteString(config.getPayloadSize());
    }

    private static final int SEQ_PER_NODE = 100;

    public static Map<Dot, Clock<MaxInt>> dotToConf(Integer nodeNumber) {
        Map<Dot, Clock<MaxInt>> result = new HashMap<>();

        // create dots
        List<Dot> dots = new ArrayList<>();
        for (Integer id = 0; id < nodeNumber; id++) {
            for (Long seq = 1L; seq <= SEQ_PER_NODE; seq++) {
                dots.add(new Dot(id, seq));
            }
        }

        List<Dot> deps = new ArrayList<>(dots);
        for (Dot dot : dots) {
            // for each dot, take a random subset of all dots as conf
            Integer numberOfDeps = RANDOM().nextInt(nodeNumber + 1);
            Collections.shuffle(deps);

            Clock<MaxInt> conf = new Clock<>(nodeNumber, new MaxInt());
            for (int i = 0; i < numberOfDeps; i++) {
                conf.addDot(deps.get(i));
            }
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

    public static ByteString randomClientKey() {
        return randomByteString(KEY_SIZE);
    }

    private static ThreadLocalRandom RANDOM() {
        return ThreadLocalRandom.current();
    }

    private static ByteString hash(Integer client, ByteString key, Config config) {
        Integer conflicts = config.getConflicts();

        if (conflicts > 200) {
            // we'll just have a single zipf object,
            // assuming the config never changes during an experiment
            if (ZIPF == null) {
                // in order to have a coefficient of:
                // - 0.75, set conflicts to 275
                // - 1.50, set conflicts to 350
                double zipfCoef = (conflicts - 200) / 100;
                ZIPF = new ZipfDistribution(ZIPF_ELEMENTS, zipfCoef);
            }
            return intToByteString(ZIPF.sample());
        } else if (conflicts == 142) {
            // two classes of clients
            if ((client + 1) % 2 == 0) {
                return BLACK;
            } else {
                return key;
            }
        } else if (conflicts <= 100) {
            if (RANDOM().nextInt(100) < conflicts) {
                return BLACK;
            } else {
                // try to avoid conflicts with client random key
                return key;
            }
        } else {
            throw new RuntimeException("conflict rate " + conflicts + " not supported!");
        }
    }

    private static ByteString repeat(byte b, Integer size) {
        byte[] ba = new byte[size];
        for (int i = 0; i < size; i++) {
            ba[i] = b;
        }
        return bs(ba);
    }

    private static ByteString randomByteString(Integer size) {
        byte[] ba = new byte[size];
        RANDOM().nextBytes(ba);
        return bs(ba);
    }

    private static ByteString bs(byte[] ba) {
        return ByteString.copyFrom(ba);
    }

    private static ByteString intToByteString(int value) {
        return ByteString.copyFrom(intToByteArray(value));
    }

    private static byte[] intToByteArray(int value) {
        return new byte[]{
            (byte) (value >>> 24),
            (byte) (value >>> 16),
            (byte) (value >>> 8),
            (byte) value};
    }
}
