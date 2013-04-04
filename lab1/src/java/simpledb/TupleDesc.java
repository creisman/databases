package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import simpledb.TupleDesc.TDItem;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable, Iterable<TDItem> {

    private static final long serialVersionUID = 1L;

    private final List<TDItem> fields;

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified types, with anonymous
     * (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this TupleDesc. It must contain at least one
     *            entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, null);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the specified types, with associated named
     * fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this TupleDesc. It must contain at least one
     *            entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        fields = new ArrayList<TDItem>(typeAr.length);

        for (int i = 0; i < typeAr.length; i++) {
            // Grab the name or the empty string if unnamed.
            String name = fieldAr != null ? fieldAr[i] : "";
            fields.add(new TDItem(typeAr[i], name));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return fields.size();
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
    public String getFieldName(int i) {
        if (i < 0 || i >= fields.size()) {
            throw new NoSuchElementException();
        }

        return fields.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) {
        if (i < 0 || i >= fields.size()) {
            throw new NoSuchElementException();
        }

        return fields.get(i).fieldType;
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
    public int fieldNameToIndex(String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).fieldName.equals(name)) {
                return i;
            }
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note that tuples from a given TupleDesc
     *         are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (int i = 0; i < fields.size(); i++) {
            size += getFieldType(i).getLen();
        }

        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields, with the first td1.numFields coming
     * from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] types = new Type[td1.numFields() + td2.numFields()];
        String[] names = new String[td1.numFields() + td2.numFields()];

        for (int i = 0; i < td1.numFields(); i++) {
            types[i] = td1.getFieldType(i);
            names[i] = td1.getFieldName(i);
        }

        for (int i = 0; i < td2.numFields(); i++) {
            types[td1.numFields() + i] = td2.getFieldType(i);
            names[td1.numFields() + i] = td2.getFieldName(i);
        }

        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two TupleDescs are considered equal if they are
     * the same size and if the n-th type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) {
            return false;
        }

        TupleDesc tmp = (TupleDesc) o;

        if (numFields() != tmp.numFields()) {
            return false;
        }

        for (int i = 0; i < numFields(); i++) {
            if (!fields.get(i).equals(tmp.fields.get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (int i = 0; i < numFields(); i++) {
            hash = hash * 37 + fields.get(i).hashCode();
        }

        return hash;
    }

    /**
     * @return An iterator which iterates over all the field TDItems that are included in this TupleDesc
     * */
    @Override
    public Iterator<TDItem> iterator() {
        return fields.iterator();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();

        str.append(fields.get(0));
        for (int i = 1; i < numFields(); i++) {
            str.append(", ").append(fields.get(i));
        }

        return str.toString();
    }

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
            fieldName = n;
            fieldType = t;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TDItem)) {
                return false;
            }

            TDItem tmp = (TDItem) o;

            return fieldName == tmp.fieldName && fieldType == tmp.fieldType;
        }

        public int hashcode() {
            return fieldName.hashCode() + fieldType.hashCode();
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
		}
	}
}
