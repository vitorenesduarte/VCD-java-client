package org.imdea.vcd;

import com.google.protobuf.ByteString;
import java.util.concurrent.ThreadLocalRandom;
import org.imdea.vcd.datum.Proto.Message;
import org.imdea.vcd.datum.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public class RandomMessageSet {

    private static final Integer KEY_SIZE = 8;
    private static final Integer MIN_ASCII = 33;
    private static final Integer MAX_ASCII = 126;
    private static final String CHARACTERS = chars(MIN_ASCII, MAX_ASCII);

    public static MessageSet generate() {
        return generate(RANDOM().nextInt(100), RANDOM().nextInt(100));
    }

    public static MessageSet generate(Config config) {
        return generate(config.getConflictPercentage(), config.getPayloadSize());
    }

    public static MessageSet generate(Integer conflictPercentage, Integer payloadSize) {
        MessageSet.Builder builder = MessageSet.newBuilder();

        Message m = Message.newBuilder()
                .setHash(randomHash(conflictPercentage, payloadSize))
                .setData(randomBytString(payloadSize))
                .build();
        builder.addMessages(m);

        builder.setStatus(MessageSet.Status.START);

        return builder.build();
    }

    private static ThreadLocalRandom RANDOM() {
        return ThreadLocalRandom.current();
    }

    private static ByteString randomBytString(Integer payloadSize) {
        StringBuilder sb = new StringBuilder(payloadSize);
        for (int i = 0; i < payloadSize; i++) {
            sb.append(CHARACTERS.charAt(RANDOM().nextInt(CHARACTERS.length())));
        }

        return bs(sb.toString());
    }

    private static ByteString randomHash(Integer conflictPercentage, Integer payloadSize) {
        if (conflictPercentage == 0) {
            return randomBytString(KEY_SIZE);
        } else {
            Integer numberOfOps = 100 / conflictPercentage;
            String hash = "" + RANDOM().nextInt(numberOfOps);
            return bs(hash);
        }
    }
    
    private static ByteString bs(String s){
        return ByteString.copyFrom(s.getBytes());
    }

    private static String chars(Integer min, Integer max) {
        StringBuilder sb = new StringBuilder();
        for (int i = min; i <= max; i++) {
            char c = (char) i;
            sb.append(c);
        }
        return sb.toString();
    }
}
