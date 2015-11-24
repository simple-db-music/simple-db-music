package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    
    private final TransactionId tid;
    private DbFile file;
    private String alias;
    private int tableid;
    private DbFileIterator iterator;
    private boolean open;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        initializeWithTableIdAndAlias(tableid, tableAlias);
    }
    
    private void initializeWithTableIdAndAlias(int tableid, String alias) {
        this.tableid = tableid;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.iterator = this.file.iterator(this.tid);
        this.alias = alias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias() {
        return this.alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        initializeWithTableIdAndAlias(tableid, tableAlias);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.iterator.open();
        this.open = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc oldDesc = this.file.getTupleDesc();
        int numFields = oldDesc.numFields();
        Type[] typeAr = new Type[numFields];
        String[] nameAr = new String[numFields];
        
        for (int i = 0; i < numFields; i++) {
            typeAr[i] = oldDesc.getFieldType(i);
            nameAr[i] = this.alias+"."+oldDesc.getFieldName(i);
        }
        
        return new TupleDesc(typeAr, nameAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this.open && this.iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return this.iterator.next();
    }

    public void close() {
        this.open = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
       this.iterator.rewind();
    }
}
