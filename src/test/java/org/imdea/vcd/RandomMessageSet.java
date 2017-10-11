package org.imdea.vcd;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.imdea.vcd.datum.Proto.Message;
import org.imdea.vcd.datum.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public class RandomMessageSet {

    private static final int ARRAY_SIZE = 1000;

    public static MessageSet generate() {
        return generate(RANDOM().nextInt(100));
    }

    public static MessageSet generate(Integer conflictPercentage) {
        MessageSet.Builder builder = MessageSet.newBuilder();

        Message m = Message.newBuilder()
                .setHash(ByteString.copyFrom(randomHash(conflictPercentage)))
                .setData(ByteString.copyFrom(randomByteBuffer()))
                .build();
        builder.addMessages(m);

        builder.setStatus(MessageSet.Status.START);

        return builder.build();
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
