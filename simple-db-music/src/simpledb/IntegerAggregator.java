package simpledb;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 * 
 * NOTE: We use BigDecimal in this class to hold the current
 * value of an aggregate thus far in its computation. BigDecimal is used
 * instead of something like an int or decimal because of the precision
 * neccesary in storing an "average so far."
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // Used if the aggregator has a grouping
    private final Map<Field, BigDecimal> groupingToAggVal;
    private final Map<Field, Integer> groupingToGroupSize;
    
    // Used if the aggregator has no grouping
    BigDecimal aggVal;
    int size;
    
    private final Type groupByFieldType;
    private final int groupByFieldIndex;
    private final int aggregateFieldIndex;
    private final Op aggregateOp;
    private TupleDesc childTupleDesc;
    private TupleDesc tupleDesc;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // Used if the aggregator has a grouping
        this.groupingToAggVal = new HashMap<Field, BigDecimal>();
        this.groupingToGroupSize = new HashMap<Field, Integer>();
        this.groupByFieldType = gbfieldtype;
        this.groupByFieldIndex = gbfield;
        this.aggregateFieldIndex = afield;
        this.aggregateOp = what;
        
        // Used if the aggregator has no grouping
        this.size = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int tupVal;
        try {
            // This cast should not fail because the IntegerAggregator is known to
            // operate on integers
            tupVal = ((IntField) tup.getField(aggregateFieldIndex)).getValue();
        } catch (ClassCastException e) {
            throw new RuntimeException("Error - column "+aggregateFieldIndex+"of tuple "+tup+" is not"
                    +"an IntField, but IntegerAggregator thinks it is!");
        }
        
        if (groupByFieldIndex == NO_GROUPING) {
            aggVal = (aggregateOp.calculateNewAgg(aggVal, tupVal, size));
            size++;
        } else {
            Field groupVal = tup.getField(groupByFieldIndex);
            int groupSize;
            BigDecimal oldAggVal = null;
            if (!groupingToGroupSize.containsKey(groupVal)) {
                groupSize = 0;
            } else {
                groupSize = groupingToGroupSize.get(groupVal);
                oldAggVal = groupingToAggVal.get(groupVal);
            }
            BigDecimal newAggVal = aggregateOp.calculateNewAgg(oldAggVal, tupVal, groupSize);
            
            groupingToGroupSize.put(groupVal, groupSize+1);
            groupingToAggVal.put(groupVal, newAggVal);
        }
        

    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        List<Tuple> tuples = new ArrayList<Tuple>();
        if (groupByFieldIndex == NO_GROUPING) {
            Tuple t = new Tuple(getTupleDesc());
            BigDecimal intDecimal = aggVal.setScale(2, RoundingMode.HALF_UP);
            t.setField(0, new IntField( ( intDecimal.intValue() )));
            tuples.add(t);
        } else {
            for (Field groupVal : groupingToAggVal.keySet()) {
                BigDecimal aggGroupVal = groupingToAggVal.get(groupVal).setScale(2, RoundingMode.HALF_UP);
                Tuple t = new Tuple(getTupleDesc());
                t.setField(0, groupVal);
                t.setField(1, new IntField(aggGroupVal.intValue()));
                tuples.add(t);
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }

    @Override
    /**
     * Returns the tuple desc for the tuples returned by the Aggregator.
     * 
     * The name of an aggregate column will be informative if setChildTupleDesc
     * has been called.
     */
    public TupleDesc getTupleDesc() {
        if (tupleDesc == null) {
            String aggColName = aggregateOp.toString();
            if (groupByFieldIndex == NO_GROUPING) {
                if (childTupleDesc != null) {
                    aggColName += " ("+childTupleDesc.getFieldName(aggregateFieldIndex)+")";
                }
                tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, 
                        new String[]{aggColName});        
            } else {
                String groupColName = "group";
                if (childTupleDesc != null) {
                    aggColName += " ("+childTupleDesc.getFieldName(aggregateFieldIndex)+")";
                    groupColName = childTupleDesc.getFieldName(groupByFieldIndex);
                }
                tupleDesc = new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE}, 
                        new String[]{groupColName, aggColName});
            }
        }
        return tupleDesc;
    }

    /**
     * Sets the tuple desc of tuples to be merged into this aggregator.
     */
    @Override
    public void setChildTupleDesc(TupleDesc td) {
        childTupleDesc = td;
        tupleDesc = null;
    }

}
