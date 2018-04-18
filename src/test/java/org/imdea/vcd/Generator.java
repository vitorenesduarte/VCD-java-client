package org.imdea.vcd;

import com.google.protobuf.ByteString;
import java.util.concurrent.ThreadLocalRandom;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public class Generator {

    private static final Integer KEY_SIZE = 8;
    private static final Integer MIN_ASCII = 33;
    private static final Integer MAX_ASCII = 126;
    private static final byte[] CHARACTERS = chars(MIN_ASCII, MAX_ASCII);

    private static final ByteString WHITE = repeat((byte) 0, 1);
    private static final ByteString BLACK = repeat((byte) 1, 1);

    public static MessageSet messageSet() {
        return messageSet("PUT", 0, randomByteString(RANDOM().nextInt(100)));
    }

    public static MessageSet messageSet(Config config) {
        return messageSet(config.getOp(), config.getConflicts(), randomByteString(config.getPayloadSize()));
    }

    public static MessageSet messageSet(Config config, ByteString data) {
        return messageSet(config.getOp(), config.getConflicts(), data);
    }

    public static MessageSet messageSet(String op, Integer conflicts, ByteString data) {
        MessageSet.Builder builder = MessageSet.newBuilder();

        Message m = Message.newBuilder()
                .setHash(hash(op, conflicts))
                .setData(data)
                .build();
        builder.addMessages(m);

        builder.setStatus(MessageSet.Status.START);

        return builder.build();
    }

    public static ByteString messageSetData(Config config) {
        return randomByteString(config.getPayloadSize());
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

    private static ByteString hash(String op, Integer conflicts) {
        ByteString hash = null;

        switch (op) {
            case "GET":
                hash = WHITE;
                break;
            case "PUT":
                if (conflicts == 0) {
                    hash = WHITE;
                } else if (conflicts == 100) {
                    // included in next condiction, but more efficient
                    hash = BLACK;
                } else if (RANDOM().nextInt(100) < conflicts) {
                    hash = BLACK;
                } else {
                    // try to avoid conflicts with random string
                    hash = randomByteString(KEY_SIZE);
                }
                break;
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
