package simpledb.utils;

import simpledb.*;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private ConcurrentHashMap<PageId, ObjLock> lockTable;
    private ConcurrentHashMap<TransactionId, ArrayList<PageId>> transactionTable;

    public LockManager(int lockTabCap, int transTabCap) {
        this.lockTable = new ConcurrentHashMap<>(lockTabCap);
        this.transactionTable = new ConcurrentHashMap<>(transTabCap);
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        ArrayList<PageId> lockList = getLockList(tid);
        return lockList != null && lockList.contains(pid);
    }

    private synchronized void block(PageId what, long start, long timeout)
            throws TransactionAbortedException {
        // activate blocking
        // lockTable.get(what).setBlocked(true);

        if (System.currentTimeMillis() - start > timeout) {
            // System.out.println(Thread.currentThread().getId() + ": aborted");
            throw new TransactionAbortedException();
        }

        try {
            wait(timeout);
            if (System.currentTimeMillis() - start > timeout) {
                // System.out.println(Thread.currentThread().getId() + ": aborted");
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            /* do nothing */
            e.printStackTrace();
        }
    }

    private synchronized void updateTransactionTable(TransactionId tid, PageId pid) {
        if (transactionTable.containsKey(tid)) {
            if (!transactionTable.get(tid).contains(pid)) {
                transactionTable.get(tid).add(pid);
            }
        } else {
            // no entry tid
            ArrayList<PageId> lockList = new ArrayList<PageId>();
            lockList.add(pid);
            transactionTable.put(tid, lockList);
        }
    }

    public synchronized void acquireLock(TransactionId tid, PageId pid, LockType reqLock, int maxTimeout)
            throws TransactionAbortedException {
        // boolean isAcquired = false;
        long start = System.currentTimeMillis();
        Random rand = new Random();
        long randomTimeout = rand.nextInt((maxTimeout - 0) + 1) + 0;
        while (true) {
            if (lockTable.containsKey(pid)) {
                // page is locked by some transaction
                if (lockTable.get(pid).getType() == LockType.SLock) {
                    if (reqLock == LockType.SLock) {
                        updateTransactionTable(tid, pid);
                        assert lockTable.get(pid).addHolder(tid) != null;
                        // isAcquired = true;
                        return;
                    } else {
                        // request XLock
                        if (transactionTable.containsKey(tid) && transactionTable.get(tid).contains(pid)
                                && lockTable.get(pid).getHolders().size() == 1) {
                            // sanity check
                            assert lockTable.get(pid).getHolders().get(0) == tid;
                            // this is a combined case when lock on pid hold only by one trans (which is exactly tid)
                            lockTable.get(pid).tryUpgradeLock(tid);
                            // isAcquired = true;
                            return;
                        } else {
                            // all need to do is just blocking
                            block(pid, start, randomTimeout);
                        }
                    }
                } else {
                    // already get a Xlock on pid
                    if (lockTable.get(pid).getHolders().get(0) == tid) {
                        // Xlock means only one holder
                        // request xlock or slock on the pid with that tid
                        // sanity check
                        assert lockTable.get(pid).getHolders().size() == 1;
                        // isAcquired = true;
                        return;
                    } else {
                        // otherwise block
                        block(pid, start, randomTimeout);
                    }
                }
            } else {
                ArrayList<TransactionId> initialHolders = new ArrayList<>();
                initialHolders.add(tid);
                lockTable.put(pid, new ObjLock(reqLock, pid, initialHolders));
                updateTransactionTable(tid, pid);
                // isAcquired = true;
                return;
            }
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {

        // remove from trans table
        if (transactionTable.containsKey(tid)) {
            transactionTable.get(tid).remove(pid);
            if (transactionTable.get(tid).size() == 0) {
                transactionTable.remove(tid);
            }
        }

        // remove from locktable
        if (lockTable.containsKey(pid)) {
            lockTable.get(pid).getHolders().remove(tid);
            if (lockTable.get(pid).getHolders().size() == 0) {
                // no more threads are waiting here
                lockTable.remove(pid);
            } else {
                // ObjLock lock = lockTable.get(pid);
                // synchronized (lock) {
                notifyAll();
                //}
            }
        }
    }

    public synchronized void releaseLocksOnTransaction(TransactionId tid) {
        if (transactionTable.containsKey(tid)) {
            PageId[] pidArr = new PageId[transactionTable.get(tid).size()];
            PageId[] toRelease = transactionTable.get(tid).toArray(pidArr);
            for (PageId pid : toRelease) {
                releaseLock(tid, pid);
            }

        }
    }

    public synchronized ArrayList<PageId> getLockList(TransactionId tid) {
        return transactionTable.getOrDefault(tid, null);
    }
}
