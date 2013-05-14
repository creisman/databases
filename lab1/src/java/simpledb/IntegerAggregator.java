package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator extends AbstractAggregator {
    private static final long serialVersionUID = 1L;

    private final Map<Field, Integer> agg;

    /**
     * NOTE: Don't use this. Use the other constructor. This is only provided to pass the tests.
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
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this(gbfield, gbfieldtype, afield, what, GROUP_BY_NAME, AGGREGATE_NAME);
    }

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
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what, String gbFieldName, String aggFieldName) {
        super(gbfield, gbfieldtype, afield, what, gbFieldName, aggFieldName);

        agg = new HashMap<Field, Integer>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        // Update the count.
        super.mergeTupleIntoGroup(tup);

        Field gb = !isGrouped() ? null : tup.getField(getGbField());
        int val = ((IntField) tup.getField(getAField())).getValue();
        if (agg.containsKey(gb)) {
            switch (getOp()) {
            // These all use a combination of sum and count.
            case SC_AVG:
            case SUM_COUNT:
            case AVG:
            case SUM:
                agg.put(gb, agg.get(gb) + val);
                break;
            case MAX:
                agg.put(gb, Math.max(agg.get(gb), val));
                break;
            case MIN:
                agg.put(gb, Math.min(agg.get(gb), val));
                break;
            default:
                break;
            }
        } else {
            // The count needs to get added even though it's not used for the iterator.
            agg.put(gb, val);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if using group, or a single
     *         (aggregateVal) if no grouping. The aggregateVal is determined by the type of aggregate specified in the
     *         constructor.
     */
    @Override
    public DbIterator iterator() {
        if (getOp() == Op.COUNT) {
            return super.iterator();
        } else if (agg.size() == 0 && !isGrouped()) {
            // Empty and not grouped should return null.
            Tuple tup = new Tuple(getTd());
            tup.setField(0, null);

            List<Tuple> list = new LinkedList<Tuple>();
            list.add(tup);
            return new TupleIterator(getTd(), list);
        }

        return new IntegerAggregatorIterator();
    }

    private class IntegerAggregatorIterator implements DbIterator {

        private static final long serialVersionUID = 1L;

        private Iterator<Entry<Field, Integer>> itr;

        /**
         * @see simpledb.DbFileIterator#open()
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            itr = agg.entrySet().iterator();
        }

        /**
         * @see simpledb.DbFileIterator#hasNext()
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return itr != null && itr.hasNext();
        }

        /**
         * @see simpledb.DbFileIterator#next()
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Entry<Field, Integer> entry = itr.next();

            Tuple tup = new Tuple(getTd());

            int val = entry.getValue();

            if (getOp() == Op.AVG) {
                val /= getCounts().get(entry.getKey());
            }

            Field valField = new IntField(val);

            if (getGbField() == NO_GROUPING) {
                tup.setField(0, valField);
            } else {
                tup.setField(0, entry.getKey());
                tup.setField(1, valField);
            }

            return tup;
        }

        /**
         * @see simpledb.DbFileIterator#rewind()
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * @see simpledb.DbFileIterator#close()
         */
        @Override
        public void close() {
            itr = null;
        }

        /**
         * @see simpledb.DbIterator#getTupleDesc()
         */
        @Override
        public TupleDesc getTupleDesc() {
            return getTd();
        }
    }
}
