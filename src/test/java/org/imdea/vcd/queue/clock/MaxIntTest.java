package org.imdea.vcd.queue.clock;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Vitor Enes
 */
public class MaxIntTest {

    @Test
    public void testContains() {
        MaxInt a = new MaxInt(10L);
        assertTrue(a.contains(9L));
        assertFalse(a.contains(11L));
    }

    @Test
    public void testMerge() {
        MaxInt a = new MaxInt(10L);
        MaxInt b = new MaxInt(17L);
        a.merge(b);
        assertTrue(a.contains(16L));
        assertTrue(a.contains(17L));
        assertFalse(a.contains(18L));
    }
}
