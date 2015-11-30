package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private final Map<Field, Integer> groupingToGroupSize;
    
    int size;
    
    private final int groupByFieldIndex;
    private final int aggregateFieldIndex;
    private final Type groupByFieldType;
    
    private TupleDesc childTupleDesc;
    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // Used if the aggregator has a grouping
        this.groupingToGroupSize = new HashMap<Field, Integer>();
        this.groupByFieldIndex = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFieldIndex = afield;
        
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Error - StringAggregator only supports COUNT op");
        }
        
        // Used if the aggregator has no grouping
        this.size = 0;
        
        this.tupleDesc = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (groupByFieldIndex == NO_GROUPING) {
            size++;
        } else {
            Field groupVal = tup.getField(groupByFieldIndex);
            int oldAggVal;
            if (!groupingToGroupSize.containsKey(groupVal)) {
                oldAggVal = 0;
            } else {
                oldAggVal = groupingToGroupSize.get(groupVal);
            }
            groupingToGroupSize.put(groupVal, oldAggVal + 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        TupleDesc td;
        List<Tuple> tuples = new ArrayList<Tuple>();
        if (groupByFieldIndex == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(size));
            tuples.add(t);
        } else {
            td = new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE}, 
                    new String[]{"groupVal", "aggregateVal"});
            for (Field groupVal : groupingToGroupSize.keySet()) {
                int count = groupingToGroupSize.get(groupVal);
                Tuple t = new Tuple(td);
                t.setField(0, groupVal);
                t.setField(1, new IntField(count));
                tuples.add(t);
            }
        }
        return new TupleIterator(td, tuples);
    }

    @Override
    public TupleDesc getTupleDesc() {
        if (tupleDesc == null) {
            String aggColName = Op.COUNT.toString();
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

    @Override
    public void setChildTupleDesc(TupleDesc td) {
        childTupleDesc = td;
        tupleDesc = null;
    }
}
