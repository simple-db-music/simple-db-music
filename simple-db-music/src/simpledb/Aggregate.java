package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator child;
    private int afield;
    private int gfield;
    private Type gfieldtype;
    private Aggregator.Op aop;
    private TupleDesc child_td;
    private Aggregator agg;
	private DbIterator it;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	// some code goes here
    	this.child = child;
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    	this.child_td = child.getTupleDesc();
    	if (gfield >= 0) {
    		this.gfieldtype = this.child_td.getFieldType(gfield);
    	} else {
    		this.gfieldtype = null;
    	}
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	// some code goes here
		if (this.gfield >= 0) {
			return this.gfield;
		}
		return Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
		// some code goes here
    	if (this.gfield >= 0) {
			return this.child_td.getFieldName(this.gfield);
		}
		return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		// some code goes here
		return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
		// some code goes here
		return this.child_td.getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		// some code goes here
		return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	// some code goes here
    	switch (this.child.getTupleDesc().getFieldType(this.afield)) {
    		case INT_TYPE:
    			this.agg = new IntegerAggregator(this.gfield, this.gfieldtype, this.afield, this.aop);
    			break;
    		case STRING_TYPE:
    			this.agg = new StringAggregator(this.gfield, this.gfieldtype, this.afield, this.aop);
    			break;
    		default:
    			throw new TransactionAbortedException();
    	}
    	child.open();
    	while (child.hasNext()) {
    		Tuple nextTuple = child.next();
    		this.agg.mergeTupleIntoGroup(nextTuple);
    	}
        it = this.agg.iterator();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
    	if (it != null && it.hasNext()) {
            return it.next();
        } else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	// some code goes here
    	it = this.agg.iterator();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
		// some code goes here
    	Type[] types;
    	String[] fields;
    	if (this.gfield == Aggregator.NO_GROUPING) {
    		types = new Type[] {Type.INT_TYPE};
        	fields = new String[] {this.aop.toString() + "(" + this.child_td.getFieldName(this.afield) + ")"};
    	} else {
    		types = new Type[] {this.child_td.getFieldType(this.gfield), Type.INT_TYPE};
        	fields = new String[] {this.child_td.getFieldName(this.gfield), this.aop.toString() + "(" + this.child_td.getFieldName(this.afield) + ")"};
    	}
    	return new TupleDesc(types, fields);
    }

    public void close() {
    	// some code goes here
    	super.close();
    	it = null;
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
