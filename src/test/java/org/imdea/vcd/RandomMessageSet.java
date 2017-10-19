package org.imdea.vcd;

import com.google.protobuf.ByteString;
import java.util.concurrent.ThreadLocalRandom;
import org.imdea.vcd.pb.Proto.Message;
import org.imdea.vcd.pb.Proto.MessageSet;

/**
 *
 * @author Vitor Enes
 */
public class RandomMessageSet {

    private static final Integer MIN_ASCII = 33;
    private static final Integer MAX_ASCII = 126;
    private static final String CHARACTERS = chars(MIN_ASCII, MAX_ASCII);

    public static MessageSet generate() {
        return generate(false, RANDOM().nextInt(100));
    }

    public static MessageSet generate(Config config) {
        return generate(config.getConflicts(), config.getPayloadSize());
    }

    public static MessageSet generate(Boolean conflicts, Integer payloadSize) {
        MessageSet.Builder builder = MessageSet.newBuilder();

        Message m = Message.newBuilder()
                .setHash(hash(conflicts))
                .setData(randomByteString(payloadSize))
                .build();
        builder.addMessages(m);

        builder.setStatus(MessageSet.Status.START);

        return builder.build();
    }

    private static ThreadLocalRandom RANDOM() {
        return ThreadLocalRandom.current();
    }

    private static ByteString randomByteString(Integer payloadSize) {
        StringBuilder sb = new StringBuilder(payloadSize);
        for (int i = 0; i < payloadSize; i++) {
            sb.append(CHARACTERS.charAt(RANDOM().nextInt(CHARACTERS.length())));
        }

        return bs(sb.toString());
    }

    private static ByteString hash(Boolean conflicts) {
        byte b = 0;
        if (conflicts) {
            b = 1;
        }

        return bs(b);
    }

    private static ByteString bs(byte b) {
        return ByteString.copyFrom(new byte[]{b});
    }

    private static ByteString bs(String s) {
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
