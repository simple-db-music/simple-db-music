package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator child;
    private final int tableid;
    private final TransactionId transactionId;
    private final TupleDesc tupleDesc;
    private boolean fetchNextCalled;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        this.transactionId = t;
        this.child = child;
        this.tableid = tableid;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Records Inserted"});
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (fetchNextCalled) {
            return null;
        }
        fetchNextCalled = true;
        int insertCount = 0;
        BufferPool bp = Database.getBufferPool();
        while (child.hasNext()) {
            try {
                bp.insertTuple(transactionId, tableid, child.next());
                insertCount++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple ans = new Tuple(tupleDesc);
        ans.setField(0, new IntField(insertCount));
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
