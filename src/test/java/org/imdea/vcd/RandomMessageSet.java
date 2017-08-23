package org.imdea.vcd;

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

    public static MessageSet generate() {
        return RandomMessageSet.generate(0);
    }

    public static MessageSet generate(Integer conflictPercentage) {
        // maximum 100 messages per set generated
        int size = RANDOM.nextInt(100);
        List<Message> messages = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            Message m = new Message(randomKey(conflictPercentage), randomString());
            messages.add(m);
        }

        return new MessageSet(messages);
    }

    private static String randomString() {
        String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        int n = alphabet.length();

        int size = RANDOM.nextInt(n);

        String result = "";
        for (int i = 0; i < size; i++) {
            result = result + alphabet.charAt(RANDOM.nextInt(n));
        }
        return result;
    }

    private static String randomKey(Integer conflictPercentage) {
        if (conflictPercentage == 0) {
            return randomString();
        } else {
            Integer numberOfOps = 100 / conflictPercentage;
            return "" + ThreadLocalRandom.current().nextLong(numberOfOps);
        }
    }
}
