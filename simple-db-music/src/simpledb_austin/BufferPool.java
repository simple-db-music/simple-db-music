package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

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
    public static final int DEFAULT_PAGES = 50;
    
    private ConcurrentHashMap<PageId, Page> pages;
    private ArrayList<PageId> cachedPageIds = new ArrayList<PageId>();
    private final int maxNumPages;
    
    private PageLockManager pageLockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.pages = new ConcurrentHashMap<PageId, Page>(numPages);
    	this.maxNumPages = numPages;
    	this.pageLockManager = new PageLockManager();
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
        // some code goes here
    	long start = System.currentTimeMillis();
    	long end = start + 300; // 300 ms blocking time
    	while (!pageLockManager.canAcquireLock(pid, tid, perm)) {
    		// block until we can acquire a lock
    		if (System.currentTimeMillis() > end) {
    			throw new TransactionAbortedException();
    		}
    	}
    	if (pageLockManager.acquireLock(pid, tid, perm)) {
    		// we have the lock on this page now
    		if (this.pages.containsKey(pid)) {
        		return this.pages.get(pid);
        	}
        	DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
    		Page page = file.readPage(pid);
    		// if cache is full, evict a page
        	if (this.pages.size() >= this.maxNumPages) {
        		this.evictPage();
        	}
        	this.pages.put(pid, page);
        	if (!cachedPageIds.contains(pid)) {
        		this.cachedPageIds.add(pid);
        	}
    		return page;
    	} else {
    		// couldn't get lock, so we try again
    		return getPage(tid, pid, perm);
    	}
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	pageLockManager.releaseLock(pid, tid);
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
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return pageLockManager.holdsLock(p, tid);
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
    	} else {
    		for (PageId pid : this.cachedPageIds) {
        		Page cachedPage = this.pages.get(pid);
        		if (cachedPage.isPageDirty() != null && cachedPage.isPageDirty().equals(tid)) {
        			// restore page to on-disk state
        			this.pages.put(pid, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
        		}
        	}
    	}
    	// release locks this transaction held
    	pageLockManager.releaseHeldLocks(tid);
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
        // some code goes here
        ArrayList<Page> dirtiedPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page p : dirtiedPages) {
        	p.markPageDirty(true, tid);
        	this.pages.put(p.getId(), p);
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
        // some code goes here
    	ArrayList<Page> dirtiedPages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
    	for (Page p : dirtiedPages) {
        	p.markPageDirty(true, tid);
        	this.pages.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
    	for (PageId pid : this.pages.keySet()) {
    		this.flushPage(pid);
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
        this.pages.remove(pid);
        this.cachedPageIds.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
    	Page p = this.pages.get(pid);
    	if (p.isPageDirty() != null) {
    		Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
        	p.markPageDirty(false, null);
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	for (PageId pid : this.cachedPageIds) {
    		Page cachedPage = this.pages.get(pid);
    		if (cachedPage.isPageDirty() != null && cachedPage.isPageDirty().equals(tid)) {
    			flushPage(pid);
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
    	for (PageId pid : this.cachedPageIds) {
    		Page cachedPage = this.pages.get(pid);
    		// evict earliest clean page
    		if (cachedPage.isPageDirty() == null) {
    			try {
					this.flushPage(pid);
				} catch (IOException e) {
					e.printStackTrace();
				}
				this.cachedPageIds.remove(pid);
    			this.pages.remove(pid);
    			return;
    		}
    	}
    	throw new DbException("All pages are dirty, cannot evict any.");
    }

}
