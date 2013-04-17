/**
 * Copyright 2013, All Rights Reserved.
 */
package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * An abstract class to keep counts of each group.
 * 
 * @author Conor
 * 
 */
public abstract class AbstractAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final Op op;

    private final Map<Field, Integer> counts;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public AbstractAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aField = afield;
        op = what;

        counts = new HashMap<Field, Integer>();

        // This is to handle the case that nothing is added.
        if (op == Op.COUNT) {
            counts.put(null, 0);
        }
    }

    /**
     * @see simpledb.Aggregator#mergeTupleIntoGroup(simpledb.Tuple)
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        // If there's no grouping put it in null, else increment its field.
        Field gf = gbField == NO_GROUPING ? null : tup.getField(gbField);
        if (counts.containsKey(gf)) {
            counts.put(gf, counts.get(gf) + 1);
        } else {
            counts.put(gf, 1);
        }
    }

    /**
     * @see simpledb.Aggregator#iterator()
     */
    @Override
    public DbIterator iterator() {
        return null;
    }

    /**
     * @return the gbfield
     */
    protected int getGbField() {
        return gbField;
    }

    /**
     * @return the gbFieldType
     */
    public Type getGbFieldType() {
        return gbFieldType;
    }

    /**
     * @return the afield
     */
    protected int getAField() {
        return aField;
    }

    /**
     * @return the op
     */
    protected Op getOp() {
        return op;
    }

    /**
     * @return the counts
     */
    protected Map<Field, Integer> getCounts() {
        return counts;
    }

}
