package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        super(gbfield, gbfieldtype, afield, what);

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

        Field gb = getGbField() == NO_GROUPING ? null : tup.getField(getGbField());
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
        List<Type> types = new ArrayList<Type>(3);
        List<String> names = new ArrayList<String>(3);

        if (getGbField() != NO_GROUPING) {
            types.add(getGbFieldType());
            names.add("groupVal");
        }

        types.add(Type.INT_TYPE);
        names.add("aggregateVal");

        TupleDesc td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));

        return new IntegerAggregatorIterator(td);
    }

    private class IntegerAggregatorIterator implements DbIterator {

        private static final long serialVersionUID = 1L;

        private final TupleDesc td;
        private Iterator<Entry<Field, Integer>> itr;
        // This is needed for the case where it's called on an empty table an must return a result.
        private boolean firstCall;

        public IntegerAggregatorIterator(TupleDesc td) {
            this.td = td;
        }

        /**
         * @see simpledb.DbFileIterator#open()
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            itr = agg.size() != 0 ? agg.entrySet().iterator() : getCounts().entrySet().iterator();
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
            // TODO oh god this is messy. How to handle empty tables with no grouping.
            // Possible splits:
            // 1) Grouping vs Non-grouping iterator.
            // 2) Empty non-grouping vs other.
            Entry<Field, Integer> entry = itr.next();

            Tuple tup = new Tuple(td);

            if (getGbField() == NO_GROUPING) {
                int val = 0;

                if (getOp() == Op.COUNT) {
                    val = getCounts().get(null);
                } else {
                    val = entry.getValue();
                }

                // If there's no grouping, the only entry will be the one I want.
                tup.setField(0, new IntField(val));
            } else {
                tup.setField(0, entry.getKey());

                int val = 0;

                if (getOp() == Op.COUNT) {
                    val = getCounts().get(entry.getKey());
                } else {
                    val = entry.getValue();

                    if (getOp() == Op.AVG) {
                        val /= getCounts().get(entry.getKey());
                    }
                }
                tup.setField(1, new IntField(val));
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
            return td;
        }
    }
}
