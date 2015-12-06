package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
    private ArrayList<Tuple> childTups = new ArrayList<Tuple>();
    private Iterator<Tuple> it;
    private boolean hasInserted = false;

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
        // some code goes here
    	this.tid = t;
    	this.child = child;
    	this.tableid = tableid;
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
        // some code goes here
    	if (it != null && !this.hasInserted) {
    		int numInserted = 0;
    		while (it.hasNext()) {
    			Tuple nextTuple = it.next();
    			try {
					Database.getBufferPool().insertTuple(this.tid, this.tableid, nextTuple);
					numInserted++;
				} catch (IOException e) {
					return null;
				}
    		}
    		Tuple tuple = new Tuple(this.td);
    		tuple.setField(0, new IntField(numInserted));
    		this.hasInserted = true;
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
        this.child = children[0];
    }
}
