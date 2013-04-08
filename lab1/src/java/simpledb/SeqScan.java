package simpledb;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads each tuple of a table in no particular
 * order (e.g., as they are laid out on disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator itr;

    /**
     * Creates a sequential scan over the specified table as a part of the specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     */
    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    /**
     * Creates a sequential scan over the specified table as a part of the specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned tupleDesc should have fields with name
     *            tableAlias.fieldName (note: this class is not responsible for handling a case where tableAlias or
     *            fieldName are null. It shouldn't crash if they are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return return the table name of the table the operator scans. This should be the actual name of the table in the
     *         catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * 
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned tupleDesc should have fields with name
     *            tableAlias.fieldName (note: this class is not responsible for handling a case where tableAlias or
     *            fieldName are null. It shouldn't crash if they are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @see simpledb.DbIterator#open()
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        if (itr == null) {
            itr = Database.getCatalog().getDbFile(tableid).iterator(tid);
            itr.open();
        }
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile, prefixed with the tableAlias string from the
     * constructor. This prefix becomes useful when joining tables containing a field(s) with the same name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile, prefixed with the tableAlias string from the
     *         constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
        TupleDesc desc = Database.getCatalog().getDbFile(tableid).getTupleDesc();
        Type[] types = new Type[desc.numFields()];
        String[] names = new String[desc.numFields()];

        for (int i = 0; i < desc.numFields(); i++) {
            types[i] = desc.getFieldType(i);
            names[i] = tableAlias + "." + desc.getFieldName(i);
        }

        return new TupleDesc(types, names);
    }

    /**
     * @see simpledb.DbIterator#hasNext()
     */
    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (itr == null) {
            return false;
        }
        return itr.hasNext();
    }

    /**
     * @see simpledb.DbIterator#next()
     */
    @Override
    public Tuple next() throws TransactionAbortedException, DbException {
        if (itr == null) {
            throw new NoSuchElementException();
        }

        return itr.next();
    }

    /**
     * @see simpledb.DbIterator#close()
     */
    @Override
    public void close() {
        if (itr != null) {
            itr.close();
        }
        itr = null;
    }

    /**
     * @see simpledb.DbIterator#rewind()
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if (itr == null) {
            throw new NoSuchElementException();
        }

        itr.rewind();
    }
}
