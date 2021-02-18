/**
 * GroupBy Operation
 **/

package qp.operators;

import qp.utils.*;

public class GroupBy extends Operator {

    int batchsize;  // Number of tuples per outbatch
    ArrayList<String> colNames;
    ArrayList<Integer> attributeIndexes;
    Operator base;  // Base operator

    /**
     * The following fields are required during
     * * execution of the distinct operator
     **/
    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer

    /**
     * constructor
     **/
    public GroupBy(Operation base, ArrayList<String> colNames, int type) {
        super(type);
        this.base = base;
        this.colNames = colNames;
        this.attributeIndexes = new ArrayList<>();
    }

    /**
     * To group the tuples based on the column names
     **/

    protected ArrayList<Attribute> convertStringsToAttributes(ArrayList<String> colNames) {
        String tblName = schema.getAttribute(0).getTabName();
        ArrayList<Attribute> attributes = new ArrayList<>();
        for (String colName : colNames) {
            Attribute attribute = new Attribute(tblName, colName);
            attributes.add(attribute);
        }
    }

    protected Map<GroupingKey, Tuple> groupBy(Batch toGroup, ArrayList<String> colNames) {

        Function<ArrayList<String>, ArrayList<Attribute>> groupingKey =
            strings -> convertStringsToAttributes(strings);
//        GroupingKey groupingKey = new GroupingKey(attributes);
        Map<GroupingKey, Tuple> groupedTuples = toGroup.stream().collect(
            Collectors.groupingBy(groupingKey));
        return groupedTuples;
    }

    /**
     * returns a batch of grouped tuples
     * * NOTE: This operation is performed on the fly
     **/
    public Batch next() {
        int i = 0;
        if (eos) {
            close();
            return null;
        }

        /** An output buffer is initiated **/
        outbatch = new Batch(batchsize);

        /** keep on checking the incoming pages until
         ** the output buffer is full
         **/
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = base.next();
                /** There is no more incoming pages from base operator **/
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

//            /** Continue this for loop until this page is fully observed
//             ** or the output buffer is full
//             **/
//            for (i = start; i < inbatch.size() && (!outbatch.isFull()); ++i) {
//                Tuple present = inbatch.get(i);
//                outbatch.add(present);
//            }

            outbatch = groupBy(inbatch, colNames);

            /** Modify the cursor to the position required
             ** when the base operator is called next time;
             **/
            if (i == inbatch.size())
                start = 0;
            else
                start = i;
        }

        return outbatch;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
//        for (int i = 0; i < attrset.size(); ++i)
//            newattr.add((Attribute) attributes.get(i).clone());
        GroupBy newgroupby = new GroupBy(newbase, newattr, optype)
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newgroupby.setSchema(newSchema);
        return newgroupby;
    }



}