/**
 * Distinct operation
 **/

package qp.operators;

import qp.utils.*;
import java.util.HashSet;
import java.util.ArrayList;

public class Distinct extends Operator {

    HashSet<Tuple> distinctTuples; // Set of Distinct tuples
//    ArrayList<Attribute> attributes;
    Operator base;
    int batchsize;
    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer

    /**
     * constructor
     **/
    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
        this.distinctTuples = new HashSet<>();
//        this.attributes = attributes;
        System.out.println("jo");
    }
//
//    public Distinct(Operator base, Condition con, int type) {
//        super(base, con, type);
//        this.distinctTuples = new HashSet<>();
//        this.attributes = new ArrayList<>();
//        System.out.println("jo");
//    }

//    public Distinct(ArrayList<Attribute> attributes, Operator base, int optype) {
//        super(optype);
//        this.base = base;
//        this.attributes = attributes;
//    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Opens the connection to the base operator
     **/
    public boolean open() {
        eos = false;  // Since the stream is just opened
        start = 0;    // Set the cursor to starting position in input buffer

        /** Set number of tuples per page**/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (base.open())
            return true;
        else
            return false;
    }

    /**
     * returns a batch of tuples that satisfies the
     * * condition specified on the tuples coming from base operator
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

            /** Continue this for loop until this page is fully observed
             ** or the output buffer is full
             **/
            for (i = start; i < inbatch.size() && (!outbatch.isFull()); ++i) {
                Tuple present = inbatch.get(i);
                /** If the condition is satisfied then
                 ** this tuple is added tot he output buffer
                 **/
                System.out.println(isDistinct(present));
                if (isDistinct(present))
                    outbatch.add(present);
            }

            /** Modify the cursor to the position requierd
             ** when the base operator is called next time;
             **/
            if (i == inbatch.size())
                start = 0;
            else
                start = i;
        }
        return outbatch;
    }


    /**
     * To check whether the current tuple is already present
     **/
    protected boolean isDistinct(Tuple tuple) {

//        ArrayList<Integer> indexes = new ArrayList<>();

//        for (int i = 0; i < attributes.size(); i++) {
//            Attribute currAttr = attributes.get(i);
//            int idx = schema.indexOf(currAttr);
//            indexes.add(idx);
//        }

        if (distinctTuples.contains(tuple)) {
            return false;
        } else {
            distinctTuples.add(tuple);
            return true;
        }
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Distinct newdist = new Distinct(newbase, optype);
        newdist.setSchema((Schema) newbase.getSchema().clone());
        return newdist;
    }

//    /**
//     * returns a batch of Distinct tuples
//     * * NOTE: This operation is performed on the fly
//     **/
//    @Override
//    public Batch next() {
//        int i = 0;
//
//        if (eos) {
//            close();
//            return null;
//        }
//
//        /** An output buffer is initiated **/
//        outbatch = new Batch(batchsize);
//        /** keep on checking the incoming pages until
//         ** the output buffer is full
//         **/
//        while (!outbatch.isFull()) {
//            if (start == 0) {
//                inbatch = base.next();
//                /** There is no more incoming pages from base operator **/
//                if (inbatch == null) {
//                    eos = true;
//                    return outbatch;
//                }
//            }
//
//            /** Continue this for loop until this page is fully observed
//             ** or the output buffer is full
//             **/
//            for (i = start; i < inbatch.size() && (!outbatch.isFull()); ++i) {
//                Tuple present = inbatch.get(i);
//                /** If the condition is satisfied then
//                 ** this tuple is added tot he output buffer
//                 **/
//                if (isDistinct(present) && checkCondition(present))
//                    outbatch.add(present);
//            }
//
//            /** Modify the cursor to the position required
//             ** when the base operator is called next time;
//             **/
//            if (i == inbatch.size())
//                start = 0;
//            else
//                start = i;
//        }
//
//        System.out.println(distinctTuples);
//        return outbatch;
//    }

}
