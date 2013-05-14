/**
 * Copyright 2013, All Rights Reserved.
 */
package simpledb;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A reentrant lock supporting both reads and writes, and upgrading from read to write. Locking is based on
 * transactions, not threads.
 * 
 * This implementation will not starve writers.
 * 
 * @author Conor
 * 
 */
public class UpgradeableReentrantReadWriteLock {
    private final Lock mutex;
    private final Condition noReaders;
    private final Condition noWriters;
    private final Set<TransactionId> readers;
    private final Set<TransactionId> writers;
    private int writersWaiting;
    private final int timeoutMin;
    private final int timeoutRange;
    private final Random rand;

    /**
     * Constructor for UpgradeableReentrantReadWriteLock.
     * 
     * @param timeoutMin
     *            - The minimum time, in ms, to wait for a lock before throwing a TransactionAbortedException. If
     *            timeout is <= 0, it will not time out.
     * @param timeoutMax
     *            - The minimum time, in ms, to wait for a lock before throwing a TransactionAbortedException. If
     *            timeout is <= 0, it will not time out.
     * 
     * @throws IllegalArgumentException
     *             if timeoutMax < timeoutMin
     */
    public UpgradeableReentrantReadWriteLock(int timeoutMin, int timeoutMax) {
        if (timeoutMax < timeoutMin) {
            throw new IllegalArgumentException();
        }

        mutex = new ReentrantLock();
        noReaders = mutex.newCondition();
        noWriters = mutex.newCondition();
        readers = new HashSet<TransactionId>();
        writers = new HashSet<TransactionId>();
        writersWaiting = 0;
        this.timeoutMin = timeoutMin;
        timeoutRange = timeoutMax - timeoutMin;
        rand = new Random();
    }

    /**
     * Gets the read lock. Blocks if not available.
     * 
     * @param tid
     *            the tid to get the read lock for.
     * @throws TransactionAbortedException
     *             - Thrown if the lock acquisition times out.
     */
    public void getReadLock(TransactionId tid) throws TransactionAbortedException {
        mutex.lock();

        if (!readers.contains(tid)) {
            // Continue only if it has the write lock or there's no writer
            // waiting or holding the lock.
            while (!writers.contains(tid) && (writersWaiting != 0 || writers.size() != 0)) {
                try {
                    if (timeoutMin <= 0) {
                        noWriters.await();
                    } else {
                        int timeout = (timeoutRange != 0 ? rand.nextInt(timeoutRange) : 0) + timeoutMin;
                        boolean ret = noWriters.await(timeout, TimeUnit.MILLISECONDS);
                        if (!ret) {
                            // I MUST release the lock.
                            mutex.unlock();
                            throw new TransactionAbortedException();
                        }
                    }
                } catch (InterruptedException ex) {
                    mutex.unlock();
                    throw new RuntimeException();
                }
            }
            readers.add(tid);
        }
        mutex.unlock();
    }

    /**
     * Releases a read lock.
     * 
     * @param tid
     *            the tid to release the lock for.
     * 
     * @throws RuntimeException
     *             if the transaction does not hold the read lock.
     */
    public void releaseReadLock(TransactionId tid) {
        mutex.lock();

        if (!readers.contains(tid)) {
            mutex.unlock();
            throw new RuntimeException("The transaction did not hold a lock.");
        }

        // Released the final lock.
        readers.remove(tid);

        if (readers.size() == 1 || readers.size() == 0) {
            // Wake up all threads in case one of them is the writer that holds
            // the last read.
            noReaders.signalAll();
        }

        mutex.unlock();
    }

    /**
     * Gets the write lock. Blocks if not available. Can be used to upgrade from a read lock.
     * 
     * @param tid
     *            the tid to get the write lock for.
     * @throws TransactionAbortedException
     *             - Thrown if the lock acquisition times out.
     */
    public void getWriteLock(TransactionId tid) throws TransactionAbortedException {
        mutex.lock();

        if (!writers.contains(tid)) {
            writersWaiting++;
            // Continue only if there are no writers and no readers, or the only reader is the
            // thread requesting the write lock.
            while (writers.size() != 0 || readers.size() != 0 && (readers.size() != 1 || !readers.contains(tid))) {
                try {
                    if (timeoutMin <= 0) {
                        noReaders.await();
                    } else {
                        int timeout = (timeoutRange != 0 ? rand.nextInt(timeoutRange) : 0) + timeoutMin;
                        boolean ret = noReaders.await(timeout, TimeUnit.MILLISECONDS);
                        if (!ret) {
                            // I MUST release the lock.
                            writersWaiting--;
                            mutex.unlock();
                            throw new TransactionAbortedException();
                        }
                    }
                } catch (InterruptedException ex) {
                    writersWaiting--;
                    mutex.unlock();
                    throw new RuntimeException();
                }
            }
            writers.add(tid);
            writersWaiting--;
        }

        mutex.unlock();
    }

    /**
     * Releases a write lock.
     * 
     * @param tid
     *            the tid to release the write lock for.
     * 
     * @throws RuntimeException
     *             if the transaction doesn't hold the lock.
     */
    public void releaseWriteLock(TransactionId tid) {
        mutex.lock();

        if (!writers.contains(tid)) {
            throw new RuntimeException("The transaction does not hold the lock");
        }

        writers.remove(tid);

        // Wake all writers if there are any. Must also wake readers in case
        // there are writers.
        noReaders.signalAll();
        noWriters.signalAll();

        mutex.unlock();
    }

    /**
     * Returns whether the transaction has a lock.
     * 
     * @param tid
     *            the transaction to check for locks for.
     * 
     * @return true if the transaction has a read or write lock.
     */
    public boolean holdsLock(TransactionId tid) {
        return readers.contains(tid) || writers.contains(tid);
    }

    /**
     * Releases any held locks for a transaction.
     * 
     * @param tid
     *            the tid to release locks for.
     */
    public void releaseLocks(TransactionId tid) {
        if (readers.contains(tid)) {
            releaseReadLock(tid);
        }

        if (writers.contains(tid)) {
            releaseWriteLock(tid);
        }
    }

    /**
     * Returns true if there is a writer.
     * 
     * @return true if a writer exists.
     */
    public boolean isWriteLocked() {
        return writers.size() != 0;
    }
}
