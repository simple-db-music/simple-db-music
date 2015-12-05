package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator child;
    private TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
    private ArrayList<Tuple> childTups = new ArrayList<Tuple>();
    private Iterator<Tuple> it;
    private boolean hasDeleted = false;

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
        // some code goes here
    	this.tid = t;
    	this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	while (child.hasNext()) {
    		childTups.add(child.next()); 		
    	}
    	it = childTups.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	it = childTups.iterator();
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
        // some code goes here
    	if (it != null && !this.hasDeleted) {
    		int numDeleted = 0;
    		while (it.hasNext()) {
    			Tuple nextTuple = it.next();
    			try {
					Database.getBufferPool().deleteTuple(this.tid, nextTuple);
					numDeleted++;
				} catch (IOException e) {
					return null;
				}
    		}
    		Tuple tuple = new Tuple(this.td);
    		tuple.setField(0, new IntField(numDeleted));
    		this.hasDeleted = true;
    		return tuple;
        } else
            return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.child = children[0];
    }

}
