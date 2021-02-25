/**
 * Distinct operation
 **/

package qp.operators;

import qp.utils.*;
import java.util.ArrayList;

public class Distinct extends Operator {

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
    Sort sortBase;
    Tuple prevTuple = null; // Keep track of previous tuple

    /**
     * constructor
     **/
    public Distinct(Operator base, int type, int numOfBuffer, ArrayList<Attribute> attributes, String tabname) {
        super(type);
        this.base = base;
        this.numOfBuffer = numOfBuffer;
        this.attributes = attributes;
        this.attributeIndexes = new ArrayList<>();
        this.tabname = tabname;

    }

    public Distinct(Operator base, int type, int numOfBuffer, String tabname) {
        super(type);
        this.base = base;
        this.numOfBuffer = numOfBuffer;
        this.attributes = new ArrayList<>();
        this.attributeIndexes = new ArrayList<>();
        this.tabname = tabname;
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

        for (int i = 0; i < attributes.size(); i++) {
            Integer idx = schema.indexOf(this.attributes.get(i));
            this.attributeIndexes.add(idx);
        }

        System.out.println(attributes.toString());

        sortBase = new Sort(base, numOfBuffer, attributes);
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
        } else if (inbatch == null) {
            inbatch = sortBase.next();
        }

        /** An output buffer is initiated **/
        outbatch = new Batch(batchsize);

        /** keep on checking the incoming pages until
         ** the output buffer is full
         **/
        while (!outbatch.isFull()) {

            /** There is no more incoming pages from base operator **/
            if (inbatch == null) {
                eos = true;
                return outbatch;
            }

            for (int i = 0; i < inbatch.size(); i++) {
                Tuple currTuple = inbatch.get(i);

                if (prevTuple == null || isDistinct(currTuple, prevTuple)) {
                    outbatch.add(currTuple);
                    prevTuple = currTuple;
                    System.out.println("curr Tuple:" + currTuple.toString());
                }
            }

            inbatch = sortBase.next();

        }

        return outbatch;
    }


    /**
     * To check whether the current tuple is already present
     **/
    protected boolean isDistinct(Tuple tuple1, Tuple tuple2) {
        int result;

        if (!this.attributeIndexes.isEmpty()) {
            for (Integer i : attributeIndexes) {
                result = Tuple.compareTuples(tuple1, tuple2, i);

                if (result != 0) {
                    return true;
                }
            }
            return false;

        } else {
            return !tuple1.equals(tuple2);
        }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        return sortBase.close();
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attributes.size(); i++)
            newattr.add((Attribute) attributes.get(i).clone());
        Distinct newDistinct = new Distinct(newbase, optype, numOfBuffer, newattr, tabname);
        newDistinct.setSchema((Schema) newbase.getSchema().clone());
        return newDistinct;
    }

//
}
