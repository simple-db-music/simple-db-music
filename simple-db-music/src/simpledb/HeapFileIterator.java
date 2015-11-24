package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

    private final HeapFile heapFile;
    private Page curPage;
    private Iterator<Tuple> curPageIterator;
    private HeapPageId curPageId;
    private final TransactionId tid;
    private boolean open;
    
    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.open = false;
    }
    
    private void setIteratorToPageNum(int pageNum) throws DbException, TransactionAbortedException {
        this.curPageId = new HeapPageId(heapFile.getId(), pageNum);
        // just getting READ access since iterator does not modify the page
        this.curPage = Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_ONLY);
        this.curPageIterator = ((HeapPage) this.curPage).iterator();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        setIteratorToPageNum(0);
        open = true;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!open) {
            return false;
        }
        while (!curPageIterator.hasNext()) {
            int curPageNum = curPageId.pageNumber();
            if (curPageNum + 1 == this.heapFile.numPages()) {
                // This is the last page and it's out of tuples
                return false;
            }
            setIteratorToPageNum(curPageNum + 1);
        }
        return true;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        return curPageIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        setIteratorToPageNum(0);
    }

    @Override
    public void close() {
        open = false;
    }

}
