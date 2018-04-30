package org.imdea.vcd.queue.clock;

import java.util.HashMap;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Vitor Enes
 */
public class ClockTest {

    @Test
    public void testContains() {
        HashMap<Integer, Long> a = new HashMap<>();
        a.put(0, 10L);
        a.put(1, 17L);
        Clock<MaxInt> clockA = Clock.vclock(a);

        assertTrue(clockA.contains(Dots.dot(0, 9L)));
        assertTrue(clockA.contains(Dots.dot(0, 10L)));
        assertFalse(clockA.contains(Dots.dot(0, 11L)));
        assertTrue(clockA.contains(Dots.dot(1, 11L)));
    }

    @Test
    public void testMerge() {
        HashMap<Integer, Long> a = new HashMap<>();
        a.put(0, 10L);
        a.put(1, 17L);
        Clock<MaxInt> clockA = Clock.vclock(a);

        HashMap<Integer, Long> b = new HashMap<>();
        b.put(0, 12L);
        b.put(1, 12L);
        Clock<MaxInt> clockB = Clock.vclock(b);

        clockA.merge(clockB);

        assertTrue(clockA.contains(Dots.dot(0, 9L)));
        assertTrue(clockA.contains(Dots.dot(0, 10L)));
        assertTrue(clockA.contains(Dots.dot(0, 11L)));
        assertTrue(clockA.contains(Dots.dot(0, 12L)));
        assertFalse(clockA.contains(Dots.dot(0, 13L)));
        assertTrue(clockA.contains(Dots.dot(1, 11L)));
        assertTrue(clockA.contains(Dots.dot(1, 17L)));
        assertFalse(clockA.contains(Dots.dot(1, 18L)));
    }

    @Test
    public void testIntersects() {
        HashMap<Integer, Long> a = new HashMap<>();
        a.put(0, 10L);
        a.put(1, 17L);
        Clock<MaxInt> clockA = Clock.vclock(a);

        assertTrue(clockA.intersects(new Dots(Dots.dot(0, 10L))));
        assertFalse(clockA.intersects(new Dots(Dots.dot(0, 11L))));
    }
}
