package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LockManager {
    
    // Maps from a page id to the set of transaction ids holding a 
    // shared lock on the page. Invariant: no page id key maps 
    // to an empty set
    private final Map<PageId, Set<TransactionId>> sharedLocks;
    // Maps from a page id to the transaction id holding an exclusive lock
    // on the page
    private final Map<PageId, TransactionId> exclusiveLock;
    
    
    // Used for aborting competing transactions when deadlock is detected
    private final Set<TransactionId> needToAbort;

    public LockManager() {
        sharedLocks = new HashMap<>();
        exclusiveLock = new HashMap<>();
        
        needToAbort = new HashSet<>();
    }
    
    /**
     * Tries to acquire a lock for the given transaction on the given page with the given permissions
     * @param tid - the transaction id of the transaction acquiring the locks
     * @param pid - the page id of the page to be locked 
     * @param perm - the desired permissions on the page to be locked
     * @return true if the lock was successfully acquired, false if not and the operation should be retried
     * @throws TransactionAbortedException if deadlock is detected and transaction needs to abort
     */
    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) 
            throws TransactionAbortedException {    
        // check if already hold appropriate lock 
        if (perm == Permissions.READ_ONLY) {
            if (holdsSharedLock(tid, pid) || holdsExclusiveLock(tid, pid)) {
                return true;
            }
        } else {
            if (holdsExclusiveLock(tid, pid)) {
                return true;
            }
        }
        
        boolean upgradingLock = false;
        if (holdsSharedLock(tid, pid)) {
            // lock upgrade
            upgradingLock = true;
        }

        long startTime = System.currentTimeMillis();
        while (needToBlock(pid, perm, upgradingLock)) {
            // Used for aborting other transactions rather than self
            /*
            if (needToAbort.contains(tid)) {
                needToAbort.remove(tid);
                throw new TransactionAbortedException();
            }
            */
            if (System.currentTimeMillis() - startTime > 300) {
                // Used for aborting other transactions rather than self
                //getCompetitingTransactions(tid, pid, perm, upgradingLock).forEach(abortId -> needToAbort.add(abortId));
                
                // Used for aborting self
                throw new TransactionAbortedException();
            }
        }

        synchronized (exclusiveLock) {
            synchronized (sharedLocks) {
                if (needToBlock(pid, perm, upgradingLock)) {
                    return false;
                }
                
                if (perm == Permissions.READ_ONLY) {
                    assert !needToBlock(pid, perm, upgradingLock);
                    Set<TransactionId> sharingSet;
                    if (sharedLocks.containsKey(pid)) {
                        sharingSet = sharedLocks.get(pid);
                    } else {
                        sharingSet = new HashSet<TransactionId>();
                    }
                    sharingSet.add(tid);
                    sharedLocks.put(pid, sharingSet);
                    assert sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid) && 
                        !exclusiveLock.containsKey(pid);
                } else {
                    assert !needToBlock(pid, perm, upgradingLock);
                    if (upgradingLock) {
                        // Invariant: no key in sharedLocks maps to an empty set
                        sharedLocks.remove(pid);
                        assert !exclusiveLock.containsKey(pid);
                    }
                    exclusiveLock.put(pid, tid);
                    assert exclusiveLock.containsKey(pid) && !sharedLocks.containsKey(pid);
                }
            }
        }
        
        return true;
    }
    
    /**
     * Returns true if transaction trying to acquire lock on page with given page id
     * needs to block (lock is not available), false otherwise
     * @param pid - the page id of the page to be locked
     * @param perm - the desired permissions on the page
     * @param upgradingLock - true if the transaction is upgrading a shared lock to an 
     * exclusive lock, false otherwise
     * @return true if the transaction needs to block (lock is not available), false otherwise
     */
    private boolean needToBlock(PageId pid, Permissions perm, boolean upgradingLock) {
        synchronized (exclusiveLock) {
            synchronized (sharedLocks) {
                if (upgradingLock) {
                    return sharedLocks.get(pid).size() > 1;
                }
                if (perm == Permissions.READ_WRITE) {
                    return exclusiveLock.containsKey(pid) || sharedLocks.containsKey(pid);
                }
                return exclusiveLock.containsKey(pid);
            }
        }
    }
    
    /**
     * If the given transaction holds a lock (exclusive or shared), releases it.
     * @param tid - the transaction id of the transaction releasing the lock
     * @param pid - the page id of the page for which the lock should be released
     */
    public void releaseLock(TransactionId tid, PageId pid) {
        synchronized (sharedLocks) {
            if (sharedLocks.containsKey(pid)) {
                Set<TransactionId> sharingSet = sharedLocks.get(pid);
                sharingSet.remove(tid);
                if (sharingSet.size() == 0) {
                    // Invariant: no key in sharedLocks maps to an empty set
                    sharedLocks.remove(pid);
                }
                return;
            }
        }
        synchronized (exclusiveLock) {
            if (exclusiveLock.containsKey(pid)) {
                exclusiveLock.remove(pid);
            }
        }
    }
    
    /**
     * Returns the set of transactions that are blocking the given transaction for obtaining a lock
     * @param tid - the transaction id of the "reference" transaction
     * @param pid - the page id of the page to be locked
     * @param perm - the desired permissions on the page to be locked
     * @param upgradingLock - true if the transaction is upgrading a shared lock to an 
     * exclusive lock, false otherwise
     * @return the set of transaction ids representing the transactions preventing the given
     * transaction from obtaining a lock
     */
    public Set<TransactionId> getCompetingTransactions (TransactionId tid, PageId pid, Permissions perm, boolean upgradingLock) {
        Set<TransactionId> competitors = new HashSet<TransactionId>();
        
        synchronized (exclusiveLock) {
            synchronized (sharedLocks) {
                if (upgradingLock) {
                    competitors.addAll(sharedLocks.get(pid));
                    competitors.remove(tid);
                } else {
                    if (exclusiveLock.containsKey(pid)) {
                        competitors.add(exclusiveLock.get(pid));
                    }
                    if (perm == Permissions.READ_WRITE) {
                        if (sharedLocks.containsKey(pid)) {
                            competitors.addAll(sharedLocks.get(pid));
                        }
                    }
                }
            }
        }
        
        return competitors;
    }
    
    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return holdsSharedLock(tid, p) || holdsExclusiveLock(tid, p);
    }
    
    /**
     * Returns true if the given transaction holds an S lock on the page with the given page id, 
     * false otherwise
     * @param tid - the transaction id of the transaction to check the lock status
     * @param p - the page id of the page to check the lock status
     * @return true if the given transaction holds an S lock on the page with the given page id, 
     * false otherwise
     */
    public boolean holdsSharedLock(TransactionId tid, PageId p) {
        synchronized (sharedLocks) {
            return sharedLocks.containsKey(p) && sharedLocks.get(p).contains(tid);
        }
    }
    
    /**
     * Returns true if the given transaction holds an X lock on the page with the given page id, 
     * false otherwise
     * @param tid - the transaction id of the transaction to check the lock status
     * @param p - the page id of the page to check the lock status
     * @return true if the given transaction holds an X lock on the page with the given page id, 
     * false otherwise
     */
    public boolean holdsExclusiveLock(TransactionId tid, PageId p) {
        synchronized (exclusiveLock) {
            return exclusiveLock.containsKey(p) && exclusiveLock.get(p).equals(tid);
        }
    }
    
    /**
     * @return the set of page ids representing all pages currently locked by some transaction
     */
    public Set<PageId> getLockedPages() {
        Set<PageId> lockedPages;

        synchronized (exclusiveLock) {
            synchronized(sharedLocks) {
                lockedPages = new HashSet<PageId>(exclusiveLock.keySet());
                lockedPages.addAll(sharedLocks.keySet());
            }
        }
        
        return lockedPages;
    }
}
