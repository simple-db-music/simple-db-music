package fingerprint;

import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import simpledb.BTreeFile;
import simpledb.DbException;
import simpledb.DbFileIterator;
import simpledb.IndexPredicate;
import simpledb.IntField;
import simpledb.Predicate.Op;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;

public abstract class Extractor {
    public abstract Set<DataPoint> extractDataPoints(double[][] spectrogram, int trackId);
    public abstract Map<Integer, Double> matchPoints(Set<DataPoint> samplePoints, BTreeFile btree, TransactionId tid) throws NoSuchElementException, DbException, TransactionAbortedException;
    
    protected Set<DataPoint> getPointsMatchingHash(int hash, BTreeFile btree, TransactionId tid) throws NoSuchElementException, DbException, TransactionAbortedException {
        Set<DataPoint> dps = new HashSet<DataPoint>();
        IndexPredicate ipred = new IndexPredicate(Op.EQUALS, new IntField(hash));
        
        DbFileIterator it = btree.indexIterator(tid, ipred);
        it.open();
        while (it.hasNext()) {
            dps.add(tupleToDataPoint(it.next()));
        }
        
        return dps;
    }
    
    protected DataPoint tupleToDataPoint(Tuple t) {
        int hash = ((IntField) t.getField(0)).getValue();
        int offset = ((IntField) t.getField(1)).getValue();
        int trackId = ((IntField) t.getField(2)).getValue();
        return new DataPoint(hash, offset, trackId);
    }
}
