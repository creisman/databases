package simpledb;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator extends AbstractAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * NOTE: Don't use this. Use the other constructor. This is only provided to pass the tests.
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException
     *             if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this(gbfield, gbfieldtype, afield, what, GROUP_BY_NAME, AGGREGATE_NAME);
    }

    /**
     * Aggregator constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            aggregation operator to use -- only supports COUNT
     * @param gbFieldName
     *            the name of the group by field name or null if no grouping
     * @param aggFieldName
     *            the name of the aggregate field name
     * 
     * @throws IllegalArgumentException
     *             if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what, String gbFieldName, String aggFieldName) {
        super(gbfield, gbfieldtype, afield, what, gbFieldName, aggFieldName);

        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
    }
}
