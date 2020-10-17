package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 * tuple是实际存储的数据
 * tuple:
 * field1(int11) filed2(int11) ... (这里是tupleDesc)
 *   12     14   ...  (这里是tuple)
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;


    // 根据构造函数
    private TupleDesc td;

    // RecordID
    private RecordId recordId;

    private List<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.td = td;
        this.fields = new ArrayList<>();
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     * 元组描述代表元组的结构
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     *         RecordID代表元组在磁盘上的位置，可以为空
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if(i >= this.fields.size())
            this.fields.add(f);
        else
            this.fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if(this.fields.size() <= i)
            return null;
        return this.fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * // 输出tuple的内容，中间用空格分隔开
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder res = new StringBuilder();
        for (Field field : this.fields) {
            res.append(field.toString());
            res.append(" ");
        }
        String ret = res.toString();
        if(fields.size() != 0)
            ret = ret.substring(0, res.length() - 1);
        return ret;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.td = td;
    }
}
