package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;

    private Type gbfieldtype;

    private int afield;

    private Op what;

    private HashMap<Object, List<Tuple>> group;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.group = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(tup.getField(afield) == null)
            return;
        if(this.gbfield == NO_GROUPING) {
            if(this.group.containsKey(NO_GROUPING)){
                this.group.get(NO_GROUPING).add(tup);
            }else {
                List<Tuple> tups = new ArrayList<>();
                tups.add(tup);
                this.group.put(NO_GROUPING, tups);
            }
        }else {
            Field field = tup.getField(this.gbfield);

            if(this.group.containsKey(field)) {
                this.group.get(field).add(tup);
            }else{
                List<Tuple> tuples = new ArrayList<>();
                tuples.add(tup);
                this.group.put(field, tuples);
            }
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private HashMap<Object, List<Tuple>> groupResult;
            private Set<Object> keySet;
            private TupleDesc td;
            private Integer aggField;
            private Op op;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.groupResult = new HashMap<>();
                this.groupResult.putAll(group);
                this.keySet = this.groupResult.keySet();
                this.op = what;
                this.aggField = afield;

                if(gbfield == NO_GROUPING) {
                    this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{""});
                } else {
                    this.td = new TupleDesc(
                            new Type[]{gbfieldtype, Type.INT_TYPE},
                            new String[]{"gfield", "afield"}
                    );
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(this.keySet.size() == 0)
                    return false;
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Tuple tup = new Tuple(this.td);
                for (Object o : this.keySet) {
                    Object key = gbfield == NO_GROUPING ? -1 : (Field)o;

                    IntField fieldValue;
                    switch (this.op) {
                        case COUNT:
                            fieldValue = new IntField(groupResult.get(key).size());
                            break;
                        case MAX:
                            fieldValue = new IntField(max(groupResult.get(key), this.aggField));
                            break;
                        case SUM:
                            fieldValue = new IntField(sum(groupResult.get(key), this.aggField));
                            break;
                        case AVG:
                            fieldValue = new IntField(avg(groupResult.get(key), this.aggField));
                            break;
                        case MIN:
                            fieldValue = new IntField(min(groupResult.get(key), this.aggField));
                            break;
                        default:
                            throw new NoSuchElementException("没有对应的操作");
                    }

                    if(gbfield == NO_GROUPING)
                        tup.setField(0, fieldValue);
                    else {
                        tup.setField(0, (Field) o);
                        tup.setField(1, fieldValue);
                    }

                    this.keySet.remove(o);
                    return tup;
                }

                throw new NoSuchElementException("没有了");

            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.groupResult = new HashMap<>();
                this.groupResult.putAll(group);
                this.keySet = this.groupResult.keySet();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return this.td;
            }

            @Override
            public void close() {
                this.groupResult.clear();
                this.keySet.clear();
                this.td = null;
            }

            private Integer max(List<Tuple> tuples, int afield) {
                int m = Integer.MIN_VALUE;
                for (Tuple tuple : tuples) {
                    IntField agg = (IntField) tuple.getField(afield);
                    if(m < agg.getValue()){
                      m = agg.getValue();
                    }
                }
                return m;
            }

            private Integer min(List<Tuple> tuples, int afield) {
                int m = Integer.MAX_VALUE;
                for (Tuple tuple : tuples) {
                    IntField agg = (IntField) tuple.getField(afield);
                    if(m > agg.getValue())
                        m = agg.getValue();
                }
                return m;
            }

            private Integer sum(List<Tuple> tuples, int afield) {
                int s = 0;
                for(Tuple tuple : tuples) {
                    IntField agg = (IntField) tuple.getField(afield);
                    s += agg.getValue();
                }
                return s;
            }

            private Integer avg(List<Tuple> tuples, int afield) {
                return sum(tuples, afield) / tuples.size();
            }
        };
    }

}
