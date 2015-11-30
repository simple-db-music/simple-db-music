package simpledb;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    static final int NO_GROUPING = -1;

    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     * */
    public enum Op implements Serializable {
        MIN {

            @Override
            public BigDecimal calculateNewAgg(BigDecimal oldAgg, int newVal,
                    int oldGroupSize) {
                if (oldGroupSize == 0 || newVal < oldAgg.intValue()) {
                    return BigDecimal.valueOf(newVal);
                }
                return oldAgg;
            }
            
        }, MAX {
            @Override
            public BigDecimal calculateNewAgg(BigDecimal oldAgg, int newVal,
                    int oldGroupSize) {
                if (oldGroupSize == 0 || newVal > oldAgg.intValue()) {
                    return BigDecimal.valueOf(newVal);
                }
                return oldAgg;
            }
        }, SUM {
            @Override
            public BigDecimal calculateNewAgg(BigDecimal oldAgg, int newVal,
                    int oldGroupSize) {
                if (oldGroupSize == 0) {
                    return BigDecimal.valueOf(newVal);
                }
                return BigDecimal.valueOf(oldAgg.intValue() + newVal);
            }
        }, AVG {
            @Override
            public BigDecimal calculateNewAgg(BigDecimal oldAgg, int newVal,
                    int oldGroupSize) {
                if (oldGroupSize == 0) {
                    return BigDecimal.valueOf(newVal);
                }
                BigDecimal num = oldAgg.multiply(BigDecimal.valueOf(oldGroupSize))
                                       .add(BigDecimal.valueOf(newVal));
                BigDecimal denom = BigDecimal.valueOf(oldGroupSize + 1);
                // Keep only 3 places after decimal
                return num.divide(denom, 3, RoundingMode.HALF_UP);
            }
        }, COUNT {
            @Override
            public BigDecimal calculateNewAgg(BigDecimal oldAgg, int newVal,
                    int oldGroupSize) {
                BigDecimal one = BigDecimal.valueOf(1);
                if (oldGroupSize == 0) {
                    return one;
                }
                return oldAgg.add(one);
            }
        },
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         * */
        SUM_COUNT {
            @Override
            public BigDecimal calculateNewAgg(BigDecimal oldAgg, int newVal,
                    int oldGroupSize) {
                throw new UnsupportedOperationException("unimplemented");
            }
        },
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         * */
        SC_AVG {
            @Override
            public BigDecimal calculateNewAgg(BigDecimal oldAgg, int newVal,
                    int oldGroupSize) {
                throw new UnsupportedOperationException("unimplemented");
            }
        };

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
        	if (this==MIN)
        		return "min";
        	if (this==MAX)
        		return "max";
        	if (this==SUM)
        		return "sum";
        	if (this==SUM_COUNT)
    			return "sum_count";
        	if (this==AVG)
        		return "avg";
        	if (this==COUNT)
        		return "count";
        	if (this==SC_AVG)
    			return "sc_avg";
        	throw new IllegalStateException("impossible to reach here");
        }
        
        /**
         * Computes the new aggregate value based on the previous aggregate value, how many values that 
         * aggregate was over, and the new value to aggregate.
         */
        public abstract BigDecimal calculateNewAgg(BigDecimal oldAgg, int newVal, int oldGroupSize);
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a DbIterator over group aggregate results.
     * @see simpledb.TupleIterator for a possible helper
     */
    public DbIterator iterator();
    
    /**
     * Returns the tuple desc for the tuples returned by the Aggregator.
     * 
     * The name of an aggregate column will be informative if setChildTupleDesc
     * has been called.
     */
    public TupleDesc getTupleDesc();
    
    /**
     * Sets the tuple desc of tuples to be merged into this aggregator.
     */
    public void setChildTupleDesc(TupleDesc td);
    
}
