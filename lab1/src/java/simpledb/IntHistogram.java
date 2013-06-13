package simpledb;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int[] counts;
    private final int min;
    private final int delta;
    private int numTuples;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives. It should split the histogram
     * into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both constant with respect to the number of
     * values being histogrammed. For example, you shouldn't simply store every value that you see in a sorted list.
     * 
     * @param buckets
     *            The number of buckets to split the input value into.
     * @param min
     *            The minimum integer value that will ever be passed to this class for histogramming
     * @param max
     *            The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.min = min;
        delta = max - min;
        numTuples = 0;

        // Since I take ints, each bucket should be at least one wide.
        counts = new int[Math.min(buckets, delta)];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * 
     * @param value
     *            Value to add to the histogram
     * 
     * @throws IllegalArgumentException
     *             if value is not between min and max.
     */
    public void addValue(int value) {
        if (value < min || value > min + delta) {
            throw new IllegalArgumentException("Value " + value + " not in range [" + min + ", " + (min + delta) + "]");
        }

        counts[findBucket(value)]++;
        numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate of the fraction of elements that are
     * greater than 5.
     * 
     * @param op
     *            Operator
     * @param v
     *            Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int bucket = findBucket(v);
        double fractionWidth;
        double selectivity;

        switch (op) {
        case LIKE:
        case EQUALS:
            return getProportion(bucket) / getBucketDelta();
        case GREATER_THAN_OR_EQ:
        case GREATER_THAN:
            selectivity = getGreaterThan(bucket);

            fractionWidth = 1 - (v - min) % getBucketDelta() / getBucketDelta();
            selectivity += fractionWidth * getProportion(bucket);

            return selectivity;
        case LESS_THAN_OR_EQ:
        case LESS_THAN:
            selectivity = getLessThan(bucket);

            fractionWidth = (v - min) % getBucketDelta() / getBucketDelta();
            selectivity += fractionWidth * getProportion(bucket);
            return selectivity;
        case NOT_EQUALS:
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        default:
            throw new IllegalArgumentException();
        }
    }

    /**
     * @return the average selectivity of this histogram.
     * 
     *         This is not an indispensable method to implement the basic join optimization. It may be needed if you
     *         want to implement a more efficient optimization
     * */
    public double avgSelectivity() {
        double sum = 0;
        for (int i = 0; i < counts.length; i++) {
            sum += getProportion(i);
        }

        return sum / counts.length;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "Min: " + min + ", Max: " + (min + delta) + ", " + Arrays.toString(counts);
    }

    private int findBucket(int value) {
        // Special case max to be in the last index.
        if (value == min + delta) {
            return counts.length - 1;
        }

        return (int) ((value - min) / getBucketDelta());
    }

    private double getBucketDelta() {
        return (double) delta / counts.length;
    }

    private double getProportion(int bucket) {
        if (bucket > counts.length - 1 || bucket < 0) {
            return 0;
        }

        return (double) counts[bucket] / numTuples;
    }

    private double getGreaterThan(int bucket) {
        if (bucket >= counts.length) {
            return 0;
        } else if (bucket < 0) {
            return 1;
        }

        double selectivity = 0;
        for (int i = bucket + 1; i < counts.length; i++) {
            selectivity += getProportion(i);
        }

        return selectivity;
    }

    private double getLessThan(int bucket) {
        if (bucket > counts.length) {
            return 1;
        } else if (bucket <= 0) {
            return 0;
        }

        double selectivity = 0;
        for (int i = bucket - 1; i >= 0; i--) {
            selectivity += getProportion(i);
        }

        return selectivity;
    }
}
