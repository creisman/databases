/**
 * Copyright 2013, All Rights Reserved.
 */
package simpledb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class manages locks for the {@link BufferPool}.
 * 
 * @author Conor
 * 
 */
public class LockManager {
    /**
     * The minimum amount of time to allow before timing out.
     */
    public static final int TIMEOUT_MIN = 100;

    /**
     * The maximum amount of time to allow before timing out.
     */
    public static final int TIMEOUT_MAX = 500;

    private final Lock locksMutator;
    private final Map<PageId, UpgradeableReentrantReadWriteLock> locks;

    /**
     * Constructor for LockManager.
     * 
     * Creates a class for managing mappings between transactions and locked pages.
     * 
     */
    public LockManager() {
        locksMutator = new ReentrantLock();
        locks = new ConcurrentHashMap<PageId, UpgradeableReentrantReadWriteLock>();
    }

    /**
     * Returns true if the given transaction has a read or write lock on the given page.
     * 
     * @param tid
     *            - the tid to look up locks for.
     * @param pid
     *            - the pid to look up locks for.
     * 
     * @return true if tid holds a read or write lock on pid.
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return locks.get(pid).holdsLock(tid);
    }

    /**
     * Returns true if the given page has a current writer.
     * 
     * @param pid
     *            - the pid to look at.
     * 
     * @return true if pid has a writer.
     */
    public boolean isWriteLocked(PageId pid) {
        return locks.get(pid).isWriteLocked();
    }

    /**
     * Obtain a lock of the given type for the given tid/pid combo.
     * 
     * @param tid
     *            - the tid to get a lock for.
     * @param pid
     *            - the pid to get a lock on.
     * @param perm
     *            - the lock type to acquire.
     * 
     * @throws TransactionAbortedException
     *             Thrown to prevent deadlocks.
     */
    public void getLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        locksMutator.lock();
        if (!locks.containsKey(pid)) {
            locks.put(pid, new UpgradeableReentrantReadWriteLock(TIMEOUT_MIN, TIMEOUT_MAX));
        }
        locksMutator.unlock();

        UpgradeableReentrantReadWriteLock lock = locks.get(pid);

        if (perm == Permissions.READ_ONLY) {
            lock.getReadLock(tid);
        } else {
            lock.getWriteLock(tid);
        }
    }

    /**
     * Releases all locks by the given tid on the given pid. Does not cause errors if locks not held.
     * 
     * @param tid
     *            - the tid to release locks for.
     * @param pid
     *            - the pid to release locks for.
     */
    public void releaseLock(TransactionId tid, PageId pid) {
        UpgradeableReentrantReadWriteLock lock = locks.get(pid);

        lock.releaseLocks(tid);
    }

    /**
     * Release locks on all pages held by the tid.
     * 
     * @param tid
     *            - the tid to release locks for.
     */
    public void releaseLocks(TransactionId tid) {
        for (UpgradeableReentrantReadWriteLock lock : locks.values()) {
            lock.releaseLocks(tid);
        }
    }
}
