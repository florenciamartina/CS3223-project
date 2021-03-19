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

    /**
     * Constructor for Distinct operator.
     * @param numOfBuffer The number of buffers available.
     * @param attributes The distinct attributes.
     * @param tabname The table name.
     */
    public Distinct(Operator base, int numOfBuffer, ArrayList<Attribute> attributes, String tabname) {
        super(OpType.DISTINCT);
        this.base = base;
        this.numOfBuffer = numOfBuffer;
        this.attributes = attributes;
        this.attributeIndexes = new ArrayList<>();
        this.tabname = tabname;

    }

    /**
     * Constructor for Distinct operator with no distinct attributes
     */
    public Distinct(Operator base, int numOfBuffer, String tabname) {
        this(base, numOfBuffer, new ArrayList<>(), tabname);
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

        for (Attribute attribute : attributes) {
            Integer idx = schema.indexOf(attribute);
            this.attributeIndexes.add(idx);
        }

        sortBase = new Sort(base, numOfBuffer, attributes, true, true);
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

            for (Tuple t: inbatch.getTuples()) {
                outbatch.add(t);
            }

            inbatch = sortBase.next();

        }

        return outbatch;
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
        Distinct newDistinct = new Distinct(newbase, numOfBuffer, newattr, tabname);
        newDistinct.setSchema((Schema) newbase.getSchema().clone());
        return newDistinct;
    }
}
