package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator {
	
	private Iterator<Tuple> i = null;
	private final TransactionId tid;
	private final int hid;
	private final int numPages;
	int currentPage = 0;
	
    public HeapFileIterator(TransactionId tid, int hid, int numPages) {
    	this.tid = tid;
    	this.hid = hid;
    	this.numPages = numPages;
    }	

    public void open() throws DbException, TransactionAbortedException {
		HeapPageId pid = new HeapPageId(this.hid, this.currentPage);
		HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
		i = p.iterator();
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
    	if (i == null) {
    		return false;
    	}
        if (i.hasNext()) {
        	return true;
        }
        // check to see if future pages have tuples
        int cp = this.currentPage;
        while (cp < numPages - 1) {
        	HeapPageId pid = new HeapPageId(this.hid, cp + 1);
    		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    		if (page.getNumEmptySlots() >= page.numSlots) {
    			cp++;
    		} else {
    			return true;
    		}
        }
        return false;
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    	if (i == null) {
    		throw new NoSuchElementException();
    	}
        if (i.hasNext()) {
        	return i.next();
        }
        if (this.currentPage < numPages - 1) {
        	this.currentPage += 1;
        	open();
        }
        return i.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        this.currentPage = 0;
        open();
    }

    public void close() {
        i = null;
    }
}
