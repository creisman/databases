package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a specified schema specified by a TupleDesc
 * object and contain Field objects with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc schema;
    private RecordId id;
    private final List<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc instance with at least one field.
     * @throws IllegalArgumentException
     *             if td is null or has 0 fields.
     */
    public Tuple(TupleDesc td) {
        if (td == null || td.numFields() <= 0) {
            throw new IllegalArgumentException();
        }
        schema = td;
        id = null;
        fields = new ArrayList<Field>(td.numFields());

        for (int i = 0; i < td.numFields(); i++) {
            fields.add(null);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be null.
     */
    public RecordId getRecordId() {
        return id;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        id = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     * @throws IllegalArgumentException
     *             thrown if the type of f doesn't match the schema.
     */
    public void setField(int i, Field f) {
        if (f != null && schema.getFieldType(i) != f.getType()) {
            throw new IllegalArgumentException();
        }

        fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     * @throws IndexOutOfBoundsException
     *             if i is not in the valid range.
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the system tests, the format needs to be as
     * follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();

        str.append(fields.get(0));
        for (int i = 1; i < fields.size(); i++) {
            str.append("\t" + fields.get(i));
        }

        return str.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return fields.iterator();
    }

    /**
     * Reset the TupleDesc of this tuple
     * 
     * @param td
     *            The TupleDesc to set.
     * 
     * @throws IllegalArgumentException
     *             if the new TupleDesc is of a different length or the current fields are not compatible with the new
     *             schema.
     * */
    public void resetTupleDesc(TupleDesc td) {
        if (td.numFields() != fields.size()) {
            throw new IllegalArgumentException();
        }

        for (int i = 0; i < fields.size(); i++) {
            if (td.getFieldType(i) != fields.get(i).getType()) {
                throw new IllegalArgumentException();
            }
        }

        schema = td;
    }
}
