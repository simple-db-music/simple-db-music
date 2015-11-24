package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator child;
    private final TransactionId transactionId;
    private final TupleDesc tupleDesc;
    private boolean fetchNextCalled;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.child = child;
        this.transactionId = t;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Records Deleted"});
        this.fetchNextCalled = false;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // do nothing
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (fetchNextCalled) {
            return null;
        }
        fetchNextCalled = true;
        int deleteCount = 0;
        BufferPool bp = Database.getBufferPool();
        while (child.hasNext()) {
            try {
                bp.deleteTuple(transactionId, child.next());
                deleteCount++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple ans = new Tuple(tupleDesc);
        ans.setField(0, new IntField(deleteCount));
        return ans;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
