package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import simpledb.utils.LockManager;
import simpledb.utils.LockType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓冲池，存储了最近访问的页。
 * 用来加快访问速度。读磁盘速度较慢，所以将部分页缓存至内存中
 * 一个缓冲池中，最多拥有numPages的页，在lab1中，超过该限制，则抛出异常，之后会实现
 * 过期淘汰策略。
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 注意线程安全, final关键字标识的变量是读线程安全的
 * @Threadsafe all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private  ConcurrentHashMap<PageId, Page> pages;

    private LockManager lockManager;
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 500;
    private static int TRANSATION_FACTOR = 2;
    private static int DEFAUT_MAXTIMEOUT = 5000;
    private final int maxPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pages = new ConcurrentHashMap<>();
        maxPages = numPages;
        this.lockManager = new LockManager(numPages, TRANSATION_FACTOR * numPages);
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * 检索具有关联权限的指定页。将获取锁，如果该锁被另一事务持有，则可能会阻塞
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     *     检索页需要在缓冲池中进行查找，如果在缓冲池中出现了，则需要返回该页。
     *     如果没有出现，则需要被加入缓冲池中，然后返回。
     *     如果当前缓冲池没有足够的空间，那么需要移除一个页，然后将新的页插入其位置
     *
     *     lab1
     *     只需要完成getPage()函数，并且不需要完成锁、事务、淘汰机制等。。所以此函数现在可以简单的写成根据PageId找page
     *     目前是在seqscan的时候会涉及到该函数。根据要求，每次查找一个页
     *        - 如果查到则返回
     *        - 如果没有查到，但是有空间，则插入缓冲池，并返回
     *        - 如果没有查到，且缓冲池没有空间，则直接抛出异常 （后续实现淘汰策略）
     *
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */

    // todo： quick access for lab4 getPage
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        LockType lockType;
        if (perm == Permissions.READ_ONLY) {
            lockType = LockType.SLock;
        } else {
            lockType = LockType.XLock;
        }
        lockManager.acquireLock(tid, pid, lockType, DEFAUT_MAXTIMEOUT);
        if(pages.containsKey(pid))
            return pages.get(pid);
        if(pages.size() >= maxPages) {
            evictPage();
        }
        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pages.put(pid, page);

        // lockManager.test(tid, pid, perm);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    // todo: quick access for lab4 releasePage
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    // todo: quick access for lab4 holdsLock
    // todo: synchronization
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            flushPages(tid);
        }

        ArrayList<PageId> lockList = lockManager.getLockList(tid);
        if (lockList != null) {
            for (PageId pid : lockList) {
                Page pg = pages.getOrDefault(pid, null);
                if (pg != null && pg.isDirty() != null) {
                    discardPage(pid);
                }
            }
        }

        // release locks finally
        lockManager.releaseLocksOnTransaction(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = file.insertTuple(tid, t);
        // 标记dirty
        for (Page page : pages) {
            page.markDirty(true, tid);
            if(this.pages.containsKey(page.getId())){
                this.pages.replace(page.getId(), page);
            }else
                this.pages.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pages = file.deleteTuple(tid, t);
        // 标记dirty
        for (Page page : pages) {
            page.markDirty(true, tid);
            if(this.pages.containsKey(page.getId())){
                this.pages.replace(page.getId(), page);
            }else
                this.pages.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Set<PageId> keySet = this.pages.keySet();
        for (PageId pageId : keySet) {
            TransactionId tid = pages.get(pageId).isDirty();
            if(tid != null){
                flushPage(pageId);
            }
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (pages.containsKey(pid)) {
            Page pg = pages.get(pid);
            if (pg.isDirty() != null) {
                // then write back
                DbFile tb = Database.getCatalog().getDatabaseFile(pg.getId().getTableId());
                tb.writePage(pg);
                pg.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        ArrayList<PageId> page2flush = lockManager.getLockList(tid);
        if (page2flush != null) {
            for (PageId p : page2flush) {
                flushPage(p);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        for (HashMap.Entry<PageId, Page> entry : pages.entrySet()) {
            PageId pid = entry.getKey();
            Page   p   = entry.getValue();
            if (p.isDirty() == null) {
                // dont need to flushpage since all page evicted are not dirty
                // flushPage(pid);
                discardPage(pid);
                return;
            }
        }
        throw new DbException("BufferPool: evictPage: all pages are marked as dirty");
    }

}
