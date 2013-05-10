package simpledb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular order. Tuples are
 * stored on pages, each of which is a fixed size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc schema;
    private final int id;

    private static Map<String, Integer> assignedIds;
    private static int nextId = 0;

    static {
        assignedIds = new HashMap<String, Integer>();
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap file.
     * @param td
     *            The schema for the table.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        schema = td;

        Integer cachedId = assignedIds.get(file.getAbsolutePath());

        // Found
        if (cachedId != null) {
            id = cachedId;
        } else { // Not found
            id = nextId++;
            assignedIds.put(file.getAbsolutePath(), id);
        }

    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to generate this tableid
     * somewhere ensure that each HeapFile has a "unique id," and that you always return the same value for a particular
     * HeapFile. We suggest hashing the absolute file name of the file underlying the heapfile, i.e.
     * f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return schema;
    }

    /**
     * @see simpledb.DbFile#readPage(simpledb.PageId)
     */
    @Override
    public Page readPage(PageId pid) {
        byte[] data = new byte[BufferPool.PAGE_SIZE];

        HeapPage page;
        RandomAccessFile input = null;
        try {
            input = new RandomAccessFile(file, "r");
            input.seek(BufferPool.PAGE_SIZE * pid.pageNumber());
            input.read(data);
            page = new HeapPage(new HeapPageId(pid.getTableId(), pid.pageNumber()), data);
        } catch (IOException ex) {
            throw new IllegalArgumentException();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ex) {
                    throw new IllegalArgumentException();
                }
            }
        }

        return page;
    }

    /**
     * @see simpledb.DbFile#writePage(simpledb.Page)
     */
    @Override
    public void writePage(Page page) throws IOException {
        byte[] data = page.getPageData();

        RandomAccessFile stream = new RandomAccessFile(file, "rw");
        stream.seek(BufferPool.PAGE_SIZE * page.getId().pageNumber());
        stream.write(data);

        stream.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     * 
     * @return The number of pages in the file.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.PAGE_SIZE);
    }

    /**
     * Writes a new empty page to the file.
     * 
     * @return The page number of the added page.
     * 
     * @throws IOException
     *             if there is a problem writing.
     */
    public synchronized int addPage() throws IOException {
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file, true);
            out.write(new byte[BufferPool.PAGE_SIZE]);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ex) {
                    throw ex;
                }
            }
        }
        return numPages() - 1;
    }

    /**
     * @see simpledb.DbFile#insertTuple(simpledb.TransactionId, simpledb.Tuple)
     */
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException,
            TransactionAbortedException {
        int pageNum = 0;

        HeapPage page = null;
        do {
            if (page != null) {
                Database.getBufferPool().releasePage(tid, page.getId());
            }

            PageId pid = new HeapPageId(id, pageNum++);

            if (pid.pageNumber() >= numPages()) {
                page = (HeapPage) Database.getBufferPool().addPage(tid, id, Permissions.READ_ONLY);
            } else {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            }
        } while (page.getNumEmptySlots() == 0);

        // Get the page with proper permissions.
        page = (HeapPage) Database.getBufferPool().getPage(tid, page.getId(), Permissions.READ_WRITE);

        page.insertTuple(t);

        ArrayList<Page> pages = new ArrayList<Page>();
        pages.add(page);

        return pages;
    }

    /**
     * @see simpledb.DbFile#deleteTuple(simpledb.TransactionId, simpledb.Tuple)
     */
    @Override
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),
                Permissions.READ_WRITE);

        page.deleteTuple(t);

        return page;
    }

    /**
     * @see simpledb.DbFile#iterator(simpledb.TransactionId)
     */
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * This iterator is greedy. It will find the next value immediately after returning the previous. This is possibly
     * less efficient if they don't do a full traversal, but it follows standard design patterns in that it hasNext() is
     * not a mutator.
     * 
     * @author Conor
     * 
     */
    private class HeapFileIterator implements DbFileIterator {

        private int nextPage;
        private Iterator<Tuple> itr;
        private final TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        /**
         * @see simpledb.DbFileIterator#open()
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            // Do nothing if already open.
            if (itr == null) {
                nextPage = 0;
                findNext();
            }
        }

        /**
         * @see simpledb.DbFileIterator#hasNext()
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (itr == null) {
                return false;
            }

            // Does nothing if already at a valid Tuple.
            findNext();

            return itr.hasNext();
        }

        private void findNext() throws DbException, TransactionAbortedException {
            while (nextPage < numPages() && (itr == null || !itr.hasNext())) {
                getNextPage();
                nextPage++;
            }
        }

        private void getNextPage() throws DbException, TransactionAbortedException {
            itr = Database.getBufferPool().getPage(tid, new HeapPageId(id, nextPage), Permissions.READ_ONLY).iterator();
        }

        /**
         * @see simpledb.DbFileIterator#next()
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (itr == null) {
                throw new NoSuchElementException();
            }

            Tuple tup = itr.next();
            findNext();

            return tup;
        }

        /**
         * @see simpledb.DbFileIterator#rewind()
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (itr == null) {
                throw new NoSuchElementException();
            }

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
    }
}
