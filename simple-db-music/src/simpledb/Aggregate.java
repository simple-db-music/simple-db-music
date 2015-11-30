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
    private final int groupByFieldIndex;
    private final int aggregateFieldIndex;
    private final Aggregator.Op aggregateOp;
    private TupleDesc tupleDesc;
    private DbIterator it;
    private Aggregator aggregator;

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
        this.child = child;
        if (gfield == -1) {
            this.groupByFieldIndex = Aggregator.NO_GROUPING;
        } else {
            this.groupByFieldIndex = gfield;
        }
        this.aggregateFieldIndex = afield;
        this.aggregateOp = aop;
        this.tupleDesc = child.getTupleDesc();
        
        setupAggregator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return groupByFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        if (groupByFieldIndex == Aggregator.NO_GROUPING) {
            return null;
        }
        return tupleDesc.getFieldName(groupByFieldIndex);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return aggregateFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return tupleDesc.getFieldName(aggregateFieldIndex);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return aggregateOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }
    
    /**
     * Sets up the aggregator by creating either an IntegerAggregator or StringAggregator
     * depending on the type of the column being aggregated. 
     */
    private void setupAggregator() {
        Type groupByType = null;
        if (groupByFieldIndex != Aggregator.NO_GROUPING) {
            groupByType = tupleDesc.getFieldType(groupByFieldIndex);
        }
        if (tupleDesc.getFieldType(aggregateFieldIndex) == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(groupByFieldIndex, groupByType, aggregateFieldIndex, aggregateOp);
        } else {
            aggregator = new StringAggregator(groupByFieldIndex, groupByType, aggregateFieldIndex, aggregateOp);
        }
        aggregator.setChildTupleDesc(tupleDesc);
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        // Set up aggregator again just in case this is not the first time
        // the aggregator has been opened. If so, the aggregator will be replaced
        // by a "fresh" one with no tuples merged in
        setupAggregator();
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        it = aggregator.iterator();
        it.open();
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
        if (it != null && it.hasNext()) {
            return it.next();
        }
        return null;
     }

    public void rewind() throws DbException, TransactionAbortedException {
        if (it == null) {
            throw new TransactionAbortedException();
        }
        it.rewind();
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
        return aggregator.getTupleDesc();
    }

    public void close() {
        super.close();
        child.close();
        it = null;
        aggregator = null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
        tupleDesc = child.getTupleDesc();
        setupAggregator();
    }
    
}
