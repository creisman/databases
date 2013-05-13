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
	public static final int TIMEOUT = 100;
	
    private final Lock locksMutator;
    private final Map<PageId, UpgradeableReentrantReadWriteLock> locks;

    public LockManager() {
        locksMutator = new ReentrantLock();
        locks = new ConcurrentHashMap<PageId, UpgradeableReentrantReadWriteLock>();
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return locks.get(pid).holdsLock(tid);
    }

    public void getLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        locksMutator.lock();
        if (!locks.containsKey(pid)) {
            locks.put(pid, new UpgradeableReentrantReadWriteLock(TIMEOUT));
        }
        locksMutator.unlock();

        UpgradeableReentrantReadWriteLock lock = locks.get(pid);

        if (perm == Permissions.READ_ONLY) {
            lock.getReadLock(tid);
        } else {
            lock.getWriteLock(tid);
        }
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        UpgradeableReentrantReadWriteLock lock = locks.get(pid);

        lock.releaseLocks(tid);
    }

    public void releaseLocks(TransactionId tid) {
        for (UpgradeableReentrantReadWriteLock lock : locks.values()) {
            lock.releaseLocks(tid);
        }
    }
}
