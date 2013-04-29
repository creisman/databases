package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private final TransactionId tid;
    private final int tableId;
    private final TupleDesc desc;

    private boolean hasRun;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid) throws DbException {
        tid = t;
        this.child = child;
        tableId = tableid;
        desc = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "rowsAffected" });
        hasRun = false;
    }

    /**
     * @see simpledb.Operator#getTupleDesc()
     */
    @Override
    public TupleDesc getTupleDesc() {
        return desc;
    }

    /**
     * @see simpledb.Operator#open()
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        child.open();
        hasRun = false;
        super.open();
    }

    /**
     * @see simpledb.Operator#close()
     */
    @Override
    public void close() {
        super.close();
        hasRun = true;
        child.close();
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
     * Inserts tuples read from child into the tableid specified by the constructor. It returns a one field tuple
     * containing the number of inserted records. Inserts should be passed through BufferPool. An instances of
     * BufferPool is available via Database.getBufferPool(). Note that insert DOES NOT need check to see if a particular
     * tuple is a duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasRun) {
            return null;
        }

        hasRun = true;

        int count = 0;
        while (child.hasNext()) {
            count++;

            try {
                Database.getBufferPool().insertTuple(tid, tableId, child.next());
            } catch (IOException ex) {
                throw new DbException(ex.getMessage());
            }
        }

        Tuple tup = new Tuple(desc);
        tup.setField(0, new IntField(count));

        return tup;
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
