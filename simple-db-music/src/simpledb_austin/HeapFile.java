package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private final File file;
	private final TupleDesc TD;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file = f;
    	this.TD = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.TD;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
			RandomAccessFile raf = new RandomAccessFile(this.file, "r");
			byte[] buffer = new byte[BufferPool.getPageSize()];
			int start = BufferPool.getPageSize() * pid.pageNumber();
			try {
				raf.seek(start);
				raf.read(buffer);
				raf.close();
				return new HeapPage((HeapPageId) pid, buffer);
			} catch (IOException e) {
				throw new IllegalArgumentException();
			}
			
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException();
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
		int start = BufferPool.getPageSize() * page.getId().pageNumber();
		raf.seek(start);
		raf.write(page.getPageData());
		raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	ArrayList<Page> modPages = new ArrayList<Page>();
    	for (int i=0; i<this.numPages(); i++) {
    		HeapPageId pid = new HeapPageId(this.getId(), i);
    		HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    		try {
    			p.insertTuple(t);
    			modPages.add(p);
    			return modPages;
    		} catch (Exception e) {
    			// this page is full, release lock on it and continue
    			Database.getBufferPool().releasePage(tid, p.getId());
    			continue;
    		}
    	}
    	// all pages were full, we must create a new one
    	HeapPageId newPid = new HeapPageId(this.getId(), this.numPages());
    	byte[] newPageBytes = HeapPage.createEmptyPageData();
    	HeapPage newPage = new HeapPage(newPid, newPageBytes);
    	newPage.insertTuple(t);
    	modPages.add(newPage);
    	FileOutputStream fos = new FileOutputStream(this.file, true);
    	fos.write(newPage.getPageData());
    	fos.close();
    	return modPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	ArrayList<Page> modPages = new ArrayList<Page>();
    	HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	p.deleteTuple(t);
    	modPages.add(p);
    	return modPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, getId(), numPages());
    }

}

