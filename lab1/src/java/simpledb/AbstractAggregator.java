/**
 * Copyright 2013, All Rights Reserved.
 */
package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * An abstract class to keep counts of each group.
 * 
 * @author Conor
 * 
 */
public abstract class AbstractAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbField;
    private final int aField;
    private final Op op;
    private final TupleDesc td;

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
     * @param gbFieldName
     *            the name of the group by field name or null if no grouping
     * @param aggFieldName
     *            the name of the aggregate field name
     */
    public AbstractAggregator(int gbfield, Type gbfieldtype, int afield, Op what, String gbFieldName,
            String aggFieldName) {
        gbField = gbfield;
        aField = afield;
        op = what;

        counts = new HashMap<Field, Integer>();

        // This is to handle the case that nothing is added.
        if (op == Op.COUNT && gbfield == NO_GROUPING) {
            counts.put(null, 0);
        }

        // Build the TupleDesc so I don't have to with every call to iterator.
        List<Type> types = new ArrayList<Type>(3);
        List<String> names = new ArrayList<String>(3);

        if (gbfield != NO_GROUPING) {
            types.add(gbfieldtype);
            names.add(gbFieldName);
        }

        types.add(Type.INT_TYPE);
        names.add(op + "(" + aggFieldName + ")");

        td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));
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
        return new CountAggregatorIterator();
    }

    /**
     * @return the gbfield
     */
    protected int getGbField() {
        return gbField;
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
     * @see simpledb.Aggregator#getTupleDesc()
     */
    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * Implemented as a wrapper since the iterators can't call getTupleDesc on the outer class.
     * 
     * @return the TupleDesc for Tuples returned by this class.
     */
    protected TupleDesc getTd() {
        return getTupleDesc();
    }

    /**
     * @return the counts
     */
    protected Map<Field, Integer> getCounts() {
        return counts;
    }

    private class CountAggregatorIterator implements DbIterator {
        private static final long serialVersionUID = 1L;

        private Iterator<Entry<Field, Integer>> itr;

        /**
         * @see simpledb.DbIterator#open()
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            itr = counts.entrySet().iterator();
        }

        /**
         * @see simpledb.DbIterator#hasNext()
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return itr != null && itr.hasNext();
        }

        /**
         * @see simpledb.DbIterator#next()
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Entry<Field, Integer> entry = itr.next();

            Tuple tup = new Tuple(getTd());
            Field val = new IntField(entry.getValue());

            if (getGbField() == NO_GROUPING) {
                tup.setField(0, val);
            } else {
                tup.setField(0, entry.getKey());
                tup.setField(1, val);
            }

            return tup;
        }

        /**
         * @see simpledb.DbIterator#rewind()
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * @see simpledb.DbIterator#getTupleDesc()
         */
        @Override
        public TupleDesc getTupleDesc() {
            return getTd();
        }

        /**
         * @see simpledb.DbIterator#close()
         */
        @Override
        public void close() {
            itr = null;
        }
    }
}
