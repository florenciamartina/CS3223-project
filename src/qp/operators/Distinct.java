/**
 * This class is to ................................
 **/

package qp.operators;

import qp.utils.*;

public class Distinct extends Operator {

    ArrayList<Attribute> attributes;

    public Distinct(ArrayList<Attribute> attributes, int type) {
        super(type);
        this.attributes = attributes;
    }

    public ArrayList<Attribute> getAttributes() {
        return attributes;
    }

    public Object clone() {
        Schema newSchema = this.getSchema();
        HashSet<Tuple> distinctTuples = new HashSet<>();


    }
}