package org.imdea.vcd;

import java.util.Arrays;
import java.util.Random;

/**
 *
 * @author Vitor Enes
 */
public class RandomMessageSet {

    public static MessageSet generate() {
        return RandomMessageSet.generate(randomString());
    }

    public static MessageSet generate(String key) {
        Message m = new Message(key, randomString());
        return new MessageSet(Arrays.asList(m));
    }

    private static String randomString() {
        String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        int n = alphabet.length();

        Random r = new Random();
        int size = r.nextInt(n);

        String result = "";
        for (int i = 0; i < size; i++) {
            result = result + alphabet.charAt(r.nextInt(n));
        }
        return result;
    }
}
