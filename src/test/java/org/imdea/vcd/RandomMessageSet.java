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

    public static MessageSet generate() {
        return generate(RANDOM().nextInt(100), RANDOM().nextInt(100));
    }

    public static MessageSet generate(Config config){
        return generate(config.getConflictPercentage(), config.getPayloadSize());
    }

    public static MessageSet generate(Integer conflictPercentage, Integer payloadSize) {
        MessageSet.Builder builder = MessageSet.newBuilder();

        Message m = Message.newBuilder()
                .setHash(ByteString.copyFrom(randomHash(conflictPercentage, payloadSize)))
                .setData(ByteString.copyFrom(randomByteBuffer(payloadSize)))
                .build();
        builder.addMessages(m);

        builder.setStatus(MessageSet.Status.START);

        return builder.build();
    }

    private static ThreadLocalRandom RANDOM() {
        return ThreadLocalRandom.current();
    }

    private static ByteBuffer randomByteBuffer(Integer payloadSize) {
        byte[] data = new byte[payloadSize];

        RANDOM().nextBytes(data);
        return ByteBuffer.wrap(data);
    }

    private static ByteBuffer randomHash(Integer conflictPercentage, Integer payloadSize) {
        if (conflictPercentage == 0) {
            return randomByteBuffer(payloadSize);
        } else {
            Integer numberOfOps = 100 / conflictPercentage;
            String hash = "" + RANDOM().nextInt(numberOfOps);
            return ByteBuffer.wrap(hash.getBytes());
        }
    }
}
