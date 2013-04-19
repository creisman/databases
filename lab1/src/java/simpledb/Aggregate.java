package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). Note that we only support aggregates
 * over a single column, grouped by a single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private final int aField;
    private final int gField;
    private final Aggregator.Op op;
    private Aggregator agg;
    private DbIterator itr;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to construct an {@link IntegerAggregator} or
     * {@link StringAggregator} to help you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or {@link Aggregator#NO_GROUPING} if there is no
     *            grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        aField = afield;
        gField = gfield;
        op = aop;

        // Load my Aggregator and TupleDesc.
        reset();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby field index in the <b>INPUT</b> tuples.
     *         If not, return {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name of the groupby field in the <b>OUTPUT</b>
     *         tuples If not, return null;
     * */
    public String groupFieldName() {
        return gField == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     * */
    public String aggregateFieldName() {
        return op + "(" + child.getTupleDesc().getFieldName(aField) + ")";
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return op;
    }

    /**
     * @param aop
     *            the op to get the name for.
     * 
     * @return The name of the given op.
     */
    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    /**
     * @see simpledb.Operator#open()
     */
    @Override
    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        if (itr == null) {
            child.open();
            while (child.hasNext()) {
                agg.mergeTupleIntoGroup(child.next());
            }
            child.close();

            itr = agg.iterator();
        }

        itr.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first field is the field by which we are grouping,
     * and the second field is the result of computing the aggregate, If there is no group by field, then the result
     * tuple should contain one field representing the result of the aggregate. Should return null if there are no more
     * tuples.
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!itr.hasNext()) {
            return null;
        }

        return itr.next();
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
     * Returns the TupleDesc of this Aggregate. If there is no group by field, this will have one field - the aggregate
     * column. If there is a group by field, the first field will be the group by field, and the second will be the
     * aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are given in the constructor, and child_td is
     * the TupleDesc of the child iterator.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return agg.getTupleDesc();
    }

    /**
     * @see simpledb.Operator#close()
     */
    @Override
    public void close() {
        super.close();

        if (itr != null) {
            itr.close();
        }
    }

    /**
     * @see simpledb.Operator#getChildren()
     */
    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { child };
    }

    /**
     * @see simpledb.Operator#setChildren(simpledb.DbIterator[])
     */
    @Override
    public void setChildren(DbIterator[] children) {
        close();
        child = children[0];
        itr = null;
        reset();
    }

    /**
     * This method resets changeable data, such as the Aggregator and TupleDesc.
     */
    private void reset() {
        TupleDesc childTd = child.getTupleDesc();
        Type gFieldType = gField == Aggregator.NO_GROUPING ? null : childTd.getFieldType(gField);
        if (childTd.getFieldType(aField) == Type.INT_TYPE) {
            agg = new IntegerAggregator(gField, gFieldType, aField, op, groupFieldName(), aggregateFieldName());
        } else {
            agg = new StringAggregator(gField, gFieldType, aField, op, groupFieldName(), aggregateFieldName());
        }
    }
}
