package simpledb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;
    private boolean noGrouping = false;
    private HashMap<Field, ArrayList<Field>> buckets = new HashMap<Field, ArrayList<Field>>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	if (gbfield == NO_GROUPING) {
    		this.noGrouping = true;
    	}
    	this.gbfieldType = gbfieldtype;
    	this.afield = afield;
    	if (what != Op.COUNT) {
    		throw new IllegalArgumentException();
    	} else {
    		this.what = what;
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field groupBy;
    	if (this.noGrouping) {
    		groupBy = new StringField("NO_GROUPING", 11);
    	} else {
    		groupBy = tup.getField(this.gbfield);
    	}
    	Field aggregate = tup.getField(this.afield);
    	if (buckets.containsKey(groupBy)) {
    		buckets.get(groupBy).add(aggregate);
    	} else {
    		ArrayList<Field> newAgg = new ArrayList<Field>();
    		newAgg.add(aggregate);
    		buckets.put(groupBy, newAgg);
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
        // some code goes here
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	Type[] types;
    	String[] fields;
    	if (this.noGrouping) {
    		types = new Type[] {Type.INT_TYPE};
        	fields = new String[] {this.what.toString()};
    	} else {
    		types = new Type[] {this.gbfieldType, Type.INT_TYPE};
        	fields = new String[] {"group", this.what.toString()};
    	}
    	TupleDesc newTD = new TupleDesc(types, fields);
        for (Field group : buckets.keySet()) {
        	int aggregateVal = 0;
        	ArrayList<Field> bucket = buckets.get(group);
        	if (this.what == Op.COUNT) {
        		aggregateVal = bucket.size();
        	}
        	Tuple newTuple = new Tuple(newTD);
        	if (this.noGrouping) {
        		newTuple.setField(0, new IntField(aggregateVal));
        	} else {
        		newTuple.setField(0, group);
            	newTuple.setField(1, new IntField(aggregateVal));
        	}
        	tuples.add(newTuple);
        }
        TupleIterator iter = new TupleIterator(newTD, tuples);
        iter.open();
        return iter;
    }

}
