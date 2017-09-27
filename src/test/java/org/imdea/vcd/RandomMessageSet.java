package org.imdea.vcd;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.imdea.vcd.datum.Message;
import org.imdea.vcd.datum.MessageSet;
import org.imdea.vcd.datum.Status;

/**
 *
 * @author Vitor Enes
 */
public class RandomMessageSet {

    private static final int MAX_SET_SIZE = 10;
    private static final int ARRAY_SIZE = 100;

    public static MessageSet generate() {
        return generate(RANDOM().nextInt(100));
    }

    public static MessageSet generate(Integer conflictPercentage) {
        return generate(conflictPercentage, MAX_SET_SIZE);
    }

    public static MessageSet generate(Integer conflictPercentage, Integer size) {
        assert size > 0;
        List<Message> messages = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            Message m = new Message(randomHash(conflictPercentage), randomByteBuffer());
            messages.add(m);
        }

        return new MessageSet(messages, Status.START);
    }

    private static ThreadLocalRandom RANDOM() {
        return ThreadLocalRandom.current();
    }

    private static ByteBuffer randomByteBuffer() {
        byte[] data = new byte[ARRAY_SIZE];

        RANDOM().nextBytes(data);
        return ByteBuffer.wrap(data);
    }

    private static ByteBuffer randomHash(Integer conflictPercentage) {
        if (conflictPercentage == 0) {
            return randomByteBuffer();
        } else {
            Integer numberOfOps = 100 / conflictPercentage;
            String hash = "" + RANDOM().nextInt(numberOfOps);
            return ByteBuffer.wrap(hash.getBytes());
        }
    }
}
