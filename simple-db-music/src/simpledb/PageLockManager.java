package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PageLockManager {
	
	private ConcurrentHashMap<PageId, ArrayList<TransactionId>> sharedLocks = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
	private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
	private final Object lock = new Object();
	
	public boolean canAcquireLock(PageId pid, TransactionId tid, Permissions perm) {
		if (perm.equals(Permissions.READ_ONLY)) {
			return canAcquireSharedLock(pid, tid);
		} else {
			return canAcquireExclusiveLock(pid, tid);
		}
	}
	
	public boolean acquireLock(PageId pid, TransactionId tid, Permissions perm) {
		if (perm.equals(Permissions.READ_ONLY)) {
			return acquireSharedLock(pid, tid);
		} else {
			return acquireExclusiveLock(pid, tid);
		}
	}
	
	private boolean canAcquireSharedLock(PageId pid, TransactionId tid) {
		if (!exclusiveLocks.containsKey(pid)) {
			return true;
		}
		// there is a chance that a race condition deletes the element, so we catch exceptions here
		try {
			boolean hasExclusiveLock = exclusiveLocks.get(pid).equals(tid);
			return hasExclusiveLock;
		// if the exclusive page lock was deleted, that means the page is free
		} catch (Exception e) {
			return true;
		}
	}
	
	private boolean canAcquireExclusiveLock(PageId pid, TransactionId tid) {
		if (!canAcquireSharedLock(pid, tid)) {
			return false;
		}
		if (sharedLocks.containsKey(pid)) {
			if (sharedLocks.get(pid).size() > 1) {
				return false;
			} else if (sharedLocks.get(pid).size() == 1) {
				// there is a chance that a race condition deletes the element, so we catch exceptions here
				try {
					boolean canUpgrade = sharedLocks.get(pid).get(0).equals(tid);
					return canUpgrade;
				} catch (Exception e) {
					return false;
				}
			} else {
				return true;
			}
		}
		return true;
	}
	
	private boolean acquireSharedLock(PageId pid, TransactionId tid) {
		synchronized(lock) {
			// check just in case another transaction acquired the lock
			if (!canAcquireSharedLock(pid, tid)) {
				return false;
			}
			if (!sharedLocks.containsKey(pid)) {
				ArrayList<TransactionId> newSharedLock = new ArrayList<TransactionId>();
				newSharedLock.add(tid);
				sharedLocks.put(pid, newSharedLock);
			} else if (!sharedLocks.get(pid).contains(tid)) {
				sharedLocks.get(pid).add(tid);
			}
			return true;
		}
	}
	
	private boolean acquireExclusiveLock(PageId pid, TransactionId tid) {
		synchronized(lock) {
			// check just in case another transaction acquired the lock
			if (!canAcquireExclusiveLock(pid, tid)) {
				return false;
			}
			// check to see if we can upgrade a shared lock
			if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).size() == 1) {
				// we know the only element must be tid, so we upgrade it
				sharedLocks.remove(tid);
			}
			if (!exclusiveLocks.containsKey(pid)) {
				exclusiveLocks.put(pid, tid);
			}
			return true;
		}
	}
	
	public void releaseLock(PageId pid, TransactionId tid) {
		synchronized(lock) {
			if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
				sharedLocks.get(pid).remove(tid);
			}
			exclusiveLocks.remove(pid, tid);
		}
	}
	
	public boolean holdsLock(PageId pid, TransactionId tid) {
		if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) {
			return true;
		}
		if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
			return true;
		}
		return false;
	}
	
	public void releaseHeldLocks(TransactionId tid) {
		synchronized(lock) {
			Iterator<Map.Entry<PageId, ArrayList<TransactionId>>> sharedIt = sharedLocks.entrySet().iterator();
			while (sharedIt.hasNext()) {
				Map.Entry<PageId, ArrayList<TransactionId>> sPair = (Map.Entry<PageId, ArrayList<TransactionId>>) sharedIt.next();
				sPair.getValue().remove(tid);
			}
			Iterator<Map.Entry<PageId, TransactionId>> exclusiveIt = exclusiveLocks.entrySet().iterator();
			while (exclusiveIt.hasNext()) {
				Map.Entry<PageId, TransactionId> exPair = (Map.Entry<PageId, TransactionId>) exclusiveIt.next();
				if (exPair.getValue().equals(tid)) {
					exclusiveIt.remove();
				}
			}
		}
	}
}
