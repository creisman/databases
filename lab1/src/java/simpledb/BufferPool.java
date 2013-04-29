package simpledb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * BufferPool manages the reading and writing of pages into memory from disk. Access methods call into it to retrieve
 * pages, and it fetches pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a page, BufferPool checks that the
 * transaction has the appropriate locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /**
     * Default number of pages passed to the constructor. This is used by other classes. BufferPool should use the
     * numPages argument to the constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int maxPages;
    private final Map<PageId, Page> pages;
    private final Queue<PageId> lru;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     * 
     * @param numPages
     *            maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        maxPages = numPages;
        pages = new ConcurrentHashMap<PageId, Page>(maxPages, 1f); // We know the exact size to make this...
        lru = new ConcurrentLinkedQueue<PageId>();
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire a lock and may block if that lock is
     * held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is present, it should be returned. If it is not
     * present, it should be added to the buffer pool and returned. If there is insufficient space in the buffer pool,
     * an page should be evicted and the new page should be added in its place.
     * 
     * @param tid
     *            the ID of the transaction requesting the page
     * @param pid
     *            the ID of the requested page
     * @param perm
     *            the requested permissions on the page
     * @return The page of the given requested table.
     * @throws TransactionAbortedException
     *             if the transaction is aborted.
     * @throws DbException
     *             if the database throws an exception.
     * @throws IllegalArgumentException
     *             if pid or perm are null.
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException,
            DbException {
        if (pid == null || perm == null) {
            throw new IllegalArgumentException();
        }

        // Resets the pid if it exists, or just adds if it doesn't.
        lru.remove(pid);
        lru.add(pid);

        if (pages.containsKey(pid)) { // If it already exists, grab it.
            return pages.get(pid);
        }

        if (pages.size() >= maxPages) {
            evictPage();
        }

        Catalog cat = Database.getCatalog();

        DbFile file = cat.getDbFile(pid.getTableId());
        Page page = file.readPage(pid);

        pages.put(pid, page);
        return page;
    }

    /**
     * Adds an empty page to the page represented by the given table id and adds it to the BufferPool.
     * 
     * @param tid
     *            The transaction to add the page for.
     * @param tableId
     *            The table to add a page to
     * @return The newly created page
     * 
     * @throws TransactionAbortedException
     *             Thrown if the transaction is aborted.
     * @throws DbException
     *             Thrown if there is an error.
     * @throws IOException
     *             Thrown if there is an IO error.
     */
    public Page addPage(TransactionId tid, int tableId) throws TransactionAbortedException, DbException, IOException {
        int pageNum = ((HeapFile) Database.getCatalog().getDbFile(tableId)).addPage();

        return getPage(tid, new HeapPageId(tableId, pageNum), Permissions.READ_WRITE);
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in wrong behavior. Think hard about who
     * needs to call this and why, and why they can run the risk of calling it.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param pid
     *            the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the transaction.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param commit
     *            a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid. Will acquire a write lock on the page the tuple is
     * added to(Lock acquisition is not needed for lab2). May block if the lock cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit, and updates cached
     * versions of any pages that have been dirtied so that future requests see up-to-date pages.
     * 
     * @param tid
     *            the transaction adding the tuple
     * @param tableId
     *            the table to add the tuple to
     * @param t
     *            the tuple to add
     * 
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException,
            TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDbFile(tableId).insertTuple(tid, t);

        for (Page page : pages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on the page the tuple is removed from.
     * May block if the lock cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit. Does not need to
     * update cached versions of any pages that have been dirtied, as it is not possible that a new page was created
     * during the deletion (note difference from addTuple).
     * 
     * @param tid
     *            the transaction deleting the tuple.
     * @param t
     *            the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        Page page = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);

        page.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes dirty data to disk so will break
     * simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pages.keySet()) {
            flushPage(pid);
        }

    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery manager to ensure that the buffer pool
     * doesn't keep a rolled back page in its cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid
     *            an ID indicating the page to flush
     * 
     * @throws IOException
     *             thrown if an IO exception occurs
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pages.get(pid);

        if (page.isDirty() != null) {
            DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId pid = lru.poll();
        try {
            flushPage(pid);
        } catch (IOException ex) {
            throw new DbException("Error while flushing page: " + ex.getMessage());
        }
        pages.remove(pid);
    }

}
