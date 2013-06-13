/**
 * Copyright 2013, All Rights Reserved.
 */
package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Vector;

import org.junit.Test;

import simpledb.CombinationEnumerator.Enumeration;

/**
 * Tests for the improved enumerateSubsets implementation.
 * 
 * @author Conor
 * 
 */
public class EnumerationTest {
    // This should be larger than 8 in order to get good timing for speedTest.
    public static final int SPEED_TEST_SIZE = 15;

    @Test
    public void testAllEnumerations() {
        int MAX = 10;
        for (int i = 1; i < MAX; i++) {
            CombinationEnumerator enumerator = new CombinationEnumerator(i, MAX);
            // There are n choose k possibilities.
            int numCombs = factorial(MAX) / (factorial(MAX - i) * factorial(i));
            int count = 0;
            while (enumerator.hasNext()) {
                enumerator.next();
                count++;
            }

            assertEquals(numCombs, count);
        }
    }

    @Test
    public void speedTest() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < SPEED_TEST_SIZE; i++) {
            CombinationEnumerator enumerator = new CombinationEnumerator(i, SPEED_TEST_SIZE);
            while (enumerator.hasNext()) {
                enumerator.next();
            }

            System.out.println("Done with " + i);
        }
        long middle = System.currentTimeMillis();
        Vector<Integer> vec = new Vector<Integer>();
        for (int i = 0; i < SPEED_TEST_SIZE; i++) {
            vec.add(i);
        }

        for (int i = 0; i < SPEED_TEST_SIZE; i++) {
            JoinOptimizer opt = new JoinOptimizer(null, null);
            opt.enumerateSubsets(vec, i);

            System.out.println("Done with " + i);
        }
        long end = System.currentTimeMillis();

        System.out.println("Original code time: " + (end - middle));
        System.out.println("My code time: " + (middle - start));
        // Make sure I at least have 8x speedup on decent sized combinations.
        assertTrue(end - middle > 8 * (middle - start));
    }

    @Test
    public void testEnumerationSize() {
        boolean[] arr = new boolean[10];
        Enumeration enu = new Enumeration(arr);

        assertEquals(0, enu.size());

        enu.set(3, true);

        assertEquals(1, enu.size());

        enu.set(5, true);
        enu.set(7, true);

        assertEquals(3, enu.size());

        enu.set(5, false);

        assertEquals(2, enu.size());
    }

    public int factorial(int num) {
        if (num <= 0) {
            return 0;
        }

        int ret = 1;
        while (num > 0) {
            ret *= num;
            num--;
        }

        return ret;
    }
}
