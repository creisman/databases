package simpledb;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate pred;
    private DbIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        pred = p;
        this.child = child;
    }

    /**
     * @return The predicate used to filter.
     */
    public Predicate getPredicate() {
        return pred;
    }

    /**
     * @see simpledb.Operator#getTupleDesc()
     */
    @Override
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    /**
     * @see simpledb.Operator#open()
     */
    @Override
    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        child.open();
        super.open();
    }

    /**
     * @see simpledb.Operator#close()
     */
    @Override
    public void close() {
        super.close();
        child.close();
    }

    /**
     * @see simpledb.DbIterator#rewind()
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    @Override
    protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
        while (child.hasNext()) {
            Tuple next = child.next();

            if (pred.filter(next)) {
                return next;
            }
        }

        return null;
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
    }
}
