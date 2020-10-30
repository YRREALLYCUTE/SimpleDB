package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;

    private Type gbfieldtype;

    private int afield;

    private HashMap<Object, List<Tuple>> group;

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
        if(!what.equals(Op.COUNT))
            throw new IllegalArgumentException("字符型只支持COUNT操作！");

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;

        this.group = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
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
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private HashMap<Object, List<Tuple>> groupResult;
            private Set<Object> keySet;
            private TupleDesc td;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.groupResult = new HashMap<>();
                this.groupResult.putAll(group);
                this.keySet = this.groupResult.keySet();

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

                    if (gbfield == NO_GROUPING)
                        tup.setField(0, new IntField(groupResult.get(-1).size()));
                    else {
                        tup.setField(0, (Field) o);
                        tup.setField(1, new IntField(groupResult.get(o).size()));
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
        };
    }

}
