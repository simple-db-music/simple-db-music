package simpledb;

import java.io.Serializable;


/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private final int leftTupleFieldNumber;
    private final int rightTupleFieldNumber;
    private final Predicate.Op comparisonOperation;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        this.leftTupleFieldNumber = field1;
        this.rightTupleFieldNumber = field2;
        this.comparisonOperation = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        Field leftVal = t1.getField(leftTupleFieldNumber);
        Field rightVal = t2.getField(rightTupleFieldNumber);
        return leftVal.compare(comparisonOperation, rightVal);
    }
    
    public int getField1() {
        return leftTupleFieldNumber; 
    }
    
    public int getField2() {
        return rightTupleFieldNumber;
    }
    
    public Predicate.Op getOperator() {
        return comparisonOperation;
    }
}
