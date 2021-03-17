/**
 * OrderBy Operation
 **/

package qp.operators;

import java.util.ArrayList;
import qp.optimizer.BufferManager;
import qp.utils.*;

public class OrderBy extends Sort {

    public OrderBy(Operator base, int numOfBuff, ArrayList<Attribute> attributeList, boolean isAsc) {
        super(base, numOfBuff, attributeList, isAsc, false);
        this.optype = OpType.ORDERBY;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        int numOfBuff = BufferManager.getBuffersPerJoin();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attributes.size(); ++i) {
            newattr.add((Attribute) attributes.get(i).clone());
        }
        OrderBy newOrderBy = new OrderBy(newbase, numOfBuff, newattr, isAsc);
        newOrderBy.setSchema((Schema) newbase.getSchema().clone());
        return newOrderBy;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

}