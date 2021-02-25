/**
 * Distinct operation
 **/

package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.*;
import java.util.HashSet;
import java.util.ArrayList;

public class Distinct extends Operator {

    ArrayList<Tuple> distinctTuples; // Set of Distinct tuples
    ArrayList<Attribute> attributes;
    ArrayList<Integer> attributeIndexes;
    Operator base;
    String tabname;
    int numOfBuffer;
    int batchsize;

    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer
    int currElementIdx = 0;
    Sort sortBase;

    /**
     * constructor
     **/
    public Distinct(Operator base, int type, ArrayList<Attribute> attributes, String tabname) {
        super(type);
        this.base = base;
        this.distinctTuples = new ArrayList<>();
        this.attributes = attributes;
        this.tabname = tabname;

    }

    public Distinct(Operator base, int type, String tabname) {
        super(type);
        this.base = base;
        this.distinctTuples = new ArrayList<>();
        this.attributes = new ArrayList<>();
        this.tabname = tabname;
        this.attributeIndexes = new ArrayList<>();
    }

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

        System.out.println(attributes.toString());

        sortBase = new Sort(OpType.EXTERNALSORT, base, numOfBuffer, attributes, tabname);
        return sortBase.open();
    }

    /**
     * returns a batch of tuples that satisfies the
     * * condition specified on the tuples coming from base operator
     * * NOTE: This operation is performed on the fly
     **/
    public Batch next() {
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

            /** There is no more incoming pages from base operator **/
            if (inbatch == null || inbatch.size() <= currElementIdx) {
                eos = true;
                return outbatch;
            }

           Tuple currTuple = inbatch.get(currElementIdx);
            if (distinctTuples.isEmpty() || isDistinct(currTuple, distinctTuples.get(distinctTuples.size() - 1))) {
                distinctTuples.add(currTuple);
            }
            currElementIdx++;

            if (currElementIdx == batchsize)
                inbatch = sortBase.next();
                currElementIdx = 0;
            }

            return outbatch;
    }


    /**
     * To check whether the current tuple is already present
     **/
    protected boolean isDistinct(Tuple tuple1, Tuple tuple2) {
        int result;
        for (int i = 0; i < attributes.size(); i++) {
            Integer idx = schema.indexOf(this.attributes.get(i));
            this.attributeIndexes.add(idx);
        }

        if (!this.attributeIndexes.isEmpty()) {
            for (Integer i : attributeIndexes) {
                result = Tuple.compareTuples(tuple1, tuple2, i);

                if (result != 0) {
                    return true;
                }
            }
            return false;

        } else {
            return tuple1.equals(tuple2);
        }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        sortBase.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) sortBase.clone();
//        int numOfBuff = BufferManager.getBuffersPerJoin();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attributes.size(); ++i)
            newattr.add((Attribute) attributes.get(i).clone());
        Distinct newDistinct = new Distinct(newbase, optype, newattr, tabname);
        newDistinct.setSchema((Schema) newbase.getSchema().clone());
        return newDistinct;
    }

//
}
