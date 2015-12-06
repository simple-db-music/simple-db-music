package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
	
	private Predicate p;
	private DbIterator child;
	private TupleDesc td;
	private ArrayList<Tuple> childTups = new ArrayList<Tuple>();
	private Iterator<Tuple> it;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	this.p = p;
    	this.child = child;
    	this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	child.open();
    	while (child.hasNext()) {
    		Tuple nextTuple = child.next();
    		if (p.filter(nextTuple)) {
    			childTups.add(nextTuple);
    		}
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
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	if (it != null && it.hasNext()) {
            return it.next();
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
