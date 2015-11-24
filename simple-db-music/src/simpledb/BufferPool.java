package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    //public static final int DEFAULT_PAGES = 50;
    public static final int DEFAULT_PAGES = 1000;
    
    private final ConcurrentMap<PageId, Page> cache;
    private final int maxPages;
    
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        System.out.println("num pages: "+numPages);
        maxPages = numPages;
        cache = new ConcurrentHashMap<PageId, Page>();
        
        lockManager = new LockManager();
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
    	BufferPool.pageSize = PAGE_SIZE;
    }
    
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        // keep trying to acquire lock, only advance once succeed
        while (!lockManager.acquireLock(tid, pid, perm)) {
        }
        
        // must synchronize on cache because eviction is possible between
        // these two lines (can evict clean pages, so even if 
        // hold the lock can't be sure another thread won't interfere
        synchronized (cache) {
            if (cache.containsKey(pid)) {
                return cache.get(pid);
            }
        }
        
        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        addPageToCache(page);
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
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid,  pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
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
        
        Set<PageId> dirtiedByTransaction = new HashSet<PageId>();
        
        // don't need to synchronize on cache - if this transaction dirtied a page,
        // must have lock on it so no cannot be evicted from the cache
        for (Entry<PageId, Page> e : cache.entrySet()) {
            Page p = e.getValue();
            if (tid.equals(p.isPageDirty())) {
                dirtiedByTransaction.add(e.getKey());
            }
        }
        
        if (commit) {
            flushPages(tid);
        }
        
        // Can remove all pages dirtied by the transaction (if committing, already flushed
        // to disk)
        for(PageId pid : dirtiedByTransaction) {
            cache.remove(pid);
        }

        // release all acquired locks
        for (PageId pid : lockManager.getLockedPages()) {
            if (lockManager.holdsLock(tid, pid)) {
                releasePage(tid, pid);
            }
        }        
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markPageDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> touchedPages = 
                Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page p : touchedPages) {
            p.markPageDirty(true, tid);
            addPageToCache(p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markPageDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        ArrayList<Page> touchedPages = 
                Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
        for (Page p : touchedPages) {
            p.markPageDirty(true, tid);
            addPageToCache(p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        synchronized (cache) {
            for (PageId pageid : cache.keySet()) {
                Page page = cache.get(pageid);
                if (page.isPageDirty() != null) {
                    flushPage(pageid);
                }
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
        cache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        int tableid = pid.getTableId();
        DbFile dbfile = Database.getCatalog().getDatabaseFile(tableid);
        Page page = cache.get(pid);
        dbfile.writePage(page);
        page.markPageDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        synchronized (cache) {
            for (Entry<PageId, Page> e : cache.entrySet()) {
                Page p = e.getValue();
                if (tid.equals(p.isPageDirty())) {
                    flushPage(e.getKey());
                }
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
  
        // Get random *clean* page to evict
        PageId pageIdToRemove;
        synchronized(cache) {
            if (cache.isEmpty()) {
                throw new DbException("Error - calling evictPage on empty cache");
            }
            
            Iterator<PageId> iterator = cache.keySet().iterator();
            do {
                if (!iterator.hasNext()) {
                    //throw new DbException("Error - need to evict page but all pages in buffer pool are dirty");
                    pageIdToRemove = cache.keySet().iterator().next();
                    break;
                }
                pageIdToRemove = iterator.next();
            } while (cache.get(pageIdToRemove).isPageDirty() != null);
            
            try {
                flushPage(pageIdToRemove);
            } catch (IOException e) {
                e.printStackTrace();
            }
            cache.remove(pageIdToRemove);
        }
    }
    
    /**
     * Adds the given page to the cache, evicting pages if necessary.
     * @param p the Page to be added to the cache
     */
    private void addPageToCache(Page p) throws DbException {
        synchronized (cache) {
            if (cache.containsValue(p)) {
                return;
            }

            while (cache.size() >= maxPages) {
                evictPage();
            }
            cache.put(p.getId(), p);
        }
    }

}