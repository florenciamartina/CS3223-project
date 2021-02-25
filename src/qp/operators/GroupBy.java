/**
 * GroupBy Operation
 **/

package qp.operators;

import qp.utils.*;
import java.util.ArrayList;
import java.util.HashMap;


public class GroupBy extends Sort {

    HashMap<Tuple, ArrayList<Tuple>> groupedTuples;

    public GroupBy(Operator base, int numOfBuff, ArrayList<Attribute> attributeList) {
        super(base, numOfBuff, attributeList);
        this.groupedTuples = new HashMap<>();
    }

}