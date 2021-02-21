package qp.operators;

import java.util.ArrayList;
import java.util.HashSet;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class ProjectDistinct extends Project {

    HashSet<Tuple> distinctTuples; // Set of Distinct tuples


    public ProjectDistinct(Operator base, ArrayList<Attribute> as, int type) {
        super(base, as, type);
        this.distinctTuples = new HashSet<>();
    }

    protected boolean isDistinct(Tuple tuple) {

        if (distinctTuples.contains(tuple)) {
            return false;
        } else {
            distinctTuples.add(tuple);
            return true;
        }
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        inbatch = base.next();

        if (inbatch == null) {
            return null;
        }

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            //Debug.PPrint(basetuple);
            //System.out.println();
            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Object data = basetuple.dataAt(attrIndex[j]);
                present.add(data);

            }

            Tuple outtuple = new Tuple(present);
            if (isDistinct(outtuple)) {
                outbatch.add(outtuple);
            }
        }

        return outbatch;
    }
}
