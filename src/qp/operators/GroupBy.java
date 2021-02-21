/**
 * GroupBy Operation
 **/

package qp.operators;

import qp.utils.*;
import java.util.ArrayList;
import java.util.HashMap;

public class GroupBy extends Operator {

    int batchsize;  // Number of tuples per outbatch
    ArrayList<Attribute> attributes;
    Operator base;  // Base operator
    HashMap<Tuple, ArrayList<Tuple>> groupedTuples = new HashMap<>();

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
    public GroupBy(Operator base, ArrayList<Attribute> attributes, int type) {
        super(type);
        this.base = base;
        this.attributes = attributes;
    }

//    /**
//     * To group the tuples based on the column names
//     **/
//
//    protected ArrayList<Attribute> convertStringsToAttributes(ArrayList<String> colNames) {
//        String tblName = schema.getAttribute(0).getTabName();
//        ArrayList<Attribute> attributes = new ArrayList<>();
//        for (String colName : colNames) {
//            Attribute attribute = new Attribute(tblName, colName);
//            attributes.add(attribute);
//        }
//    }

    protected void groupBy(Batch toGroup) {

        ArrayList<Tuple> tuples = new ArrayList<>();
        int pageSize = toGroup.getPageSize();

        for (int i = 0; i < pageSize; i++) {
            tuples.add(toGroup.get(i));
        }

        ArrayList<Tuple> keyAttr = new ArrayList<>();
        HashMap<Tuple, ArrayList<Tuple>> groupedTuples = new HashMap<>();

        for (int i = 0; i < tuples.size(); i++) {
            ArrayList<Object> temp = new ArrayList<>();
            for (int j = 0; j < attributes.size(); j++) {
                int idx = schema.indexOf(attributes.get(j));
                temp.add(tuples.get(i).dataAt(idx));
            }
            Tuple tempTuple = new Tuple(temp);
            keyAttr.add(tempTuple);
        }

        for (int i = 0; i < keyAttr.size(); i++) {
            Tuple a = keyAttr.get(i);
            if (groupedTuples.containsKey(a)) {
                ArrayList<Tuple> tupleArrayList = groupedTuples.get(a);
                tupleArrayList.add(tuples.get(i));
                groupedTuples.put(a, tupleArrayList);

            } else {
                ArrayList<Tuple> arr = new ArrayList<>();
                arr.add(tuples.get(i));
                groupedTuples.put(a, arr);
            }
        }
        this.groupedTuples = groupedTuples;

//        ArrayList<Object> tupleObj = Arrays.asList(tuples);
//
//        Function<ArrayList<Attribute>, ArrayList<Attribute>> groupingKey =
//                attributes -> attributes;
////        GroupingKey groupingKey = new GroupingKey(attributes);
//        HashMap<ArrayList<Attribute>, Tuple> groupedTuples = tupleObj.stream().collect(
//            Collectors.groupingBy(groupingKey));
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

            for ( Tuple key : groupedTuples.keySet() ) {
                ArrayList<Tuple> ts = groupedTuples.get(key);
                for (Tuple t : ts) {
                    outbatch.add(t);
                }
            }

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
        GroupBy newgroupby = new GroupBy(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newgroupby.setSchema(newSchema);
        return newgroupby;
    }

    public long getStatistics(GroupBy node) {
        return 1;
    }



}