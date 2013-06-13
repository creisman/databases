/**
 * Copyright 2013, All Rights Reserved.
 */
package simpledb;

import java.util.Arrays;
import java.util.Iterator;

/**
 * A set of classes for enumerating all combinations.
 * 
 * @author Conor
 * 
 */
public class CombinationEnumerator implements Iterator<CombinationEnumerator.Enumeration> {
    private final int choose;
    private final boolean[] combination;
    private boolean first;

    public CombinationEnumerator(int choose, int from) {
        if (choose > from) {
            throw new IllegalArgumentException();
        }

        this.choose = choose;
        combination = new boolean[from];
        first = true;
    }

    @Override
    public boolean hasNext() {
        // Check that all chosen values are in a row at the end.
        for (int i = combination.length - 1; i > combination.length - 1 - choose; i--) {
            if (!combination[i]) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Enumeration next() {
        if (first) {
            first = false;

            for (int i = 0; i < choose; i++) {
                combination[i] = true;
            }
        } else {
            int count = 0;
            // If at the end, clear them all out so I can find the next one to shift.
            if (combination[combination.length - 1]) {
                for (int i = combination.length - 1; combination[i]; i--) {
                    combination[i] = false;
                    count++;
                }
            }

            int i;
            for (i = combination.length - 1 - count; i >= 0; i--) {
                if (combination[i]) {
                    combination[i] = false;
                    combination[i + 1] = true;
                    break;
                }
            }

            // Set to next spot after newest shift.
            i += 2;
            // Replace removed ones.
            for (int j = 0; j < count; j++) {
                combination[i + j] = true;
            }
        }

        return new Enumeration(combination.clone());
    }

    /**
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public static class Enumeration {
        private final boolean[] arr;

        public Enumeration(boolean[] backing) {
            arr = backing;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Enumeration)) {
                return false;
            }

            Enumeration enu = (Enumeration) o;

            return Arrays.equals(arr, enu.arr);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(arr);
        }

        public boolean isIncluded(int index) {
            return arr[index];
        }

        public int size() {
            int count = 0;

            for (boolean element : arr) {
                if (element) {
                    count++;
                }
            }

            return count;
        }

        public void set(int index, boolean value) {
            arr[index] = value;
        }

        @Override
        public String toString() {
            return Arrays.toString(arr);
        }

        @Override
        public Enumeration clone() {
            return new Enumeration(arr.clone());
        }
    }
}
