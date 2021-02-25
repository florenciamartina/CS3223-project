/**
 * GroupBy Operation
 **/

package qp.operators;

import qp.utils.*;
import java.util.ArrayList;
import java.util.HashMap;


public class GroupBy extends Sort {

//
//    public GroupBy(int type, Operator base, int numOfBuff, ArrayList<Attribute> attributeList, String tabname) {
//        super(type, base, numOfBuff, attributeList, tabname);
////        this.groupedTuples = new HashMap<>();
//    }

    public GroupBy(Operator base, int numOfBuff, ArrayList<Attribute> attributeList) {
        super(base, numOfBuff, attributeList);
    }

}