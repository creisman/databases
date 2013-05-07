/**
 * Copyright 2013, All Rights Reserved.
 */
package simpledb;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class manages locks for the {@link BufferPool}.
 * 
 * @author Conor
 * 
 */
public class LockManager {
    private final Lock locksMutator;
    private final Map<PageId, ReadWriteLock> locks;
    private final Map<TransactionId, Set<PageId>> heldReadLocks;
    private final Map<TransactionId, Set<PageId>> heldWriteLocks;

    public LockManager() {
        locksMutator = new ReentrantLock();
        locks = new ConcurrentHashMap<PageId, ReadWriteLock>();
        heldReadLocks = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        heldWriteLocks = new ConcurrentHashMap<TransactionId, Set<PageId>>();
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return holdsReadLock(tid, pid) || holdsWriteLock(tid, pid);
    }

    public void getLock(TransactionId tid, PageId pid, Permissions perm) {
        locksMutator.lock();
        if (!locks.containsKey(pid)) {
            locks.put(pid, new ReentrantReadWriteLock());
        }
        locksMutator.unlock();

        ReadWriteLock lock = locks.get(pid);

        if (perm == Permissions.READ_ONLY) {
            if (!holdsReadLock(tid, pid)) {
                lock.readLock().lock();
                addToHeld(tid, pid, heldReadLocks);
            }
        } else {
            if (!holdsWriteLock(tid, pid)) {
                // XXX This does NOT count as an upgrade!
                if (holdsReadLock(tid, pid)) {
                    releaseReadLock(tid, pid);
                }
                lock.writeLock().lock();
                addToHeld(tid, pid, heldWriteLocks);
            }
        }
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        if (holdsReadLock(tid, pid)) {
            releaseReadLock(tid, pid);
        }

        if (holdsWriteLock(tid, pid)) {
            releaseWriteLock(tid, pid);
        }
    }

    private void addToHeld(TransactionId tid, PageId pid, Map<TransactionId, Set<PageId>> locks) {
        Set<PageId> set = locks.get(tid);
        if (set == null) {
            set = new HashSet<PageId>();
            locks.put(tid, set);
        }

        set.add(pid);
    }

    private boolean holdsReadLock(TransactionId tid, PageId pid) {
        return holdsLock(tid, pid, heldReadLocks);
    }

    private boolean holdsWriteLock(TransactionId tid, PageId pid) {
        return holdsLock(tid, pid, heldWriteLocks);
    }

    private boolean holdsLock(TransactionId tid, PageId pid, Map<TransactionId, Set<PageId>> locks) {
        Set<PageId> set = locks.get(tid);
        return set != null && set.contains(pid);
    }

    private void releaseReadLock(TransactionId tid, PageId pid) {
        heldReadLocks.get(tid).remove(pid);
        locks.get(pid).readLock().unlock();
    }

    private void releaseWriteLock(TransactionId tid, PageId pid) {
        heldWriteLocks.get(tid).remove(pid);
        locks.get(pid).writeLock().unlock();
    }
}
