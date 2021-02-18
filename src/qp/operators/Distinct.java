/**
 * Distinct operation
 **/

package qp.operators;

import qp.utils.*;
import java.util.HashSet;

public class Distinct extends Operator {

    int batchsize;  // Number of tuples per outbatch
    Operator base;  // Base operator
    HashSet<Tuple> distinctTuples; // Set of distinct tuples

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
    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
        distinctTuples = new HashSet<>();
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * To check whether the current tuple is already present
     **/
    protected boolean isDistinct(Tuple tuple) {
        if (distinctTuples.contains(tuple)) {
            return false;
        } else {
            distinctTuples.add(tuple);
            return true;
        }
    }

    /**
     * returns a batch of distinct tuples
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
                if (isDistinct(present))
                    outbatch.add(present);
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
     * closes the output connection
     * * i.e., no more pages to output
     **/
    public boolean close() {
        base.close();    // Added base.close
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Distinct newDistinct = new Distinct(newbase, optype);
        newDistinct.setSchema((Schema) newbase.getSchema().clone());
        return newDistinct;
    }
}