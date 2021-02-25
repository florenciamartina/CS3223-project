/**
 * OrderBy Operation
 **/

package qp.operators;

import java.util.ArrayList;
import qp.utils.*;

public class OrderBy extends Sort {

    public OrderBy(Operator base, int numOfBuff, ArrayList<Attribute> attributeList, boolean isAsc) {
        super(base, numOfBuff, attributeList, isAsc);
    }

}