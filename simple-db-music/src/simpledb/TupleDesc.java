package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private final List<TDItem> columns;
    
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return columns.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        ArrayList<TDItem> tempColumns = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem column;
            if (i < fieldAr.length) {
                column = new TDItem(typeAr[i], fieldAr[i]);
            } else {
                column = new TDItem(typeAr[i], null);
            }
            tempColumns.add(column);
        }
        // For immutability - TupleDesc should be fully
        // immutable
        columns = Collections.unmodifiableList(tempColumns);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String[]{});
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return columns.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= columns.size()) {
            throw new NoSuchElementException();
        }
        return columns.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= columns.size()) {
            throw new NoSuchElementException();
        }
        return columns.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name != null) {
            for (int i = 0; i < columns.size(); i++) {
                String fieldName = columns.get(i).fieldName;
                if (name.equals(fieldName)) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;
        for (TDItem tditem : columns) {
            totalSize += tditem.fieldType.getLen();
        }
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int newSize = td1.numFields()+td2.numFields();
        Type[] typeAr = new Type[newSize];
        String[] fieldNameAr = new String[newSize];
        TupleDesc td;
        int adjustedIndex;
        for (int i = 0; i < newSize; i++) {
            if (i < td1.numFields()) {
                td = td1;
                adjustedIndex = i;
            } else {
                td = td2;
                adjustedIndex = i - td1.numFields();
            }
            typeAr[i] = td.getFieldType(adjustedIndex);
            fieldNameAr[i] = td.getFieldName(adjustedIndex);
        }
        
        return new TupleDesc(typeAr, fieldNameAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        
        TupleDesc other = (TupleDesc) o;
        
        int myFieldCount = this.numFields();
        if (myFieldCount != other.numFields()) {
            return false;
        }
        
        for (int i = 0; i < myFieldCount; i++) {
            if (this.getFieldType(i) != other.getFieldType(i)) {
                return false;
            }
        }
        
        return true;
    }

    public int hashCode() {
        List<Type> types = columns.stream().map(tditem -> tditem.fieldType).collect(Collectors.toList());
        return types.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String s = "";
        for (TDItem tditem : columns) {
            s += tditem.toString()+", ";
        }
        // Remove trailing comma and space
        return s.substring(0, s.length() - 2);
    }
}
