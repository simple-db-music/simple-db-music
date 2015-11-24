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
    private final TupleDesc desc;    
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.desc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
        synchronized (file) {
            return this.file.getAbsoluteFile().hashCode();
        }
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.desc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            synchronized(file) {
                RandomAccessFile raf = new RandomAccessFile(this.file, "r");
                int pageSize = BufferPool.getPageSize();
                int offset = pid.pageNumber()*pageSize;
                raf.seek(offset);

                // Don't need to worry about running off end of the file - file
                // length will always be a multiple of page size (https://piazza.com/class/idkhyh3lqzokv?cid=71)
                byte[] data = new byte[pageSize];
                for (int i = 0; i < data.length; i++) {
                    data[i] = raf.readByte();
                }
                raf.close();

                HeapPageId id = new HeapPageId(pid.getTableId(), pid.pageNumber());
                Page page = new HeapPage(id, data);
                return page;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();
        try {
            synchronized(file) {
                RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
                int pageSize = BufferPool.getPageSize();
                int offset = pid.pageNumber()*pageSize;
                raf.seek(offset);
                raf.write(page.getPageData());
                raf.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // Math.max to ensure HeapFile will always contain at least one page
        // Can use integer division safely - file length will always be a multiple of
        // page size (https://piazza.com/class/idkhyh3lqzokv?cid=71)
        synchronized(file) {
            return Math.max(((int) this.file.length()) / BufferPool.getPageSize(), 1);
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int pageNo = 0;
        HeapPageId pageid = null;
        HeapPage page;
        
        synchronized(this) {
            int numPages = numPages();
            do {
                if (pageid != null) {
                    // can safely release lock on page with no free slots
                    // since we didn't touch it/use its data in any way
                    Database.getBufferPool().releasePage(tid, pageid);
                }
                pageid = new HeapPageId(getId(), pageNo++);
                if (pageid.pageNumber() == numPages) {
                    synchronized(file) {
                        // Need to create new page to store the tuple
                        byte[] newPageData = HeapPage.createEmptyPageData();
                        page = new HeapPage(pageid, newPageData);
                        page.insertTuple(t);

                        FileOutputStream fos = new FileOutputStream(file, true);
                        fos.write(page.getPageData());
                        fos.flush();
                        fos.close();

                        return new ArrayList<Page>(Arrays.asList(page));
                    }
                }
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageid, Permissions.READ_WRITE);
            } while (page.getNumEmptySlots() == 0);
        }
        
        page.insertTuple(t);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pageid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<Page>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

