package org.imdea.vcd;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author Vitor Enes
 */
public class RandomMessageSet {

    private static final Random RANDOM = new Random();
    private static final int MAX_SET_SIZE = 10;
    private static final int MAX_ARRAY_SIZE = 10;

    public static MessageSet generate() {
        return RandomMessageSet.generate(0);
    }

    public static MessageSet generate(Integer conflictPercentage) {
        // generate non-empty sets
        int size = Math.max(RANDOM.nextInt(MAX_SET_SIZE), 1);
        List<Message> messages = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            Message m = new Message(randomHash(conflictPercentage), randomByteBuffer());
            messages.add(m);
        }

        return new MessageSet(messages);
    }

    private static ByteBuffer randomByteBuffer() {
        int size = RANDOM.nextInt(MAX_ARRAY_SIZE);
        byte[] data = new byte[size];

        RANDOM.nextBytes(data);
        return ByteBuffer.wrap(data);
    }

    private static ByteBuffer randomHash(Integer conflictPercentage) {
        if (conflictPercentage == 0) {
            return randomByteBuffer();
        } else {
            Integer numberOfOps = 100 / conflictPercentage;
            String hash = "" + ThreadLocalRandom.current().nextLong(numberOfOps);
            return ByteBuffer.wrap(hash.getBytes());
        }
    }
}
