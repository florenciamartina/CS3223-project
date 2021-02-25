/**
 * OrderBy Operation
 **/

package qp.operators;

import java.util.ArrayList;
import qp.utils.*;

public class OrderBy extends Sort {

    public OrderBy(int type, Operator base, int numOfBuff, ArrayList<Attribute> attributeList, String tabname, boolean isAsc) {
        super(type, base, numOfBuff, attributeList, tabname, isAsc);
    }

}