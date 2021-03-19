/**
 * GroupBy Operation
 **/

package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.*;
import java.util.ArrayList;
import java.util.HashMap;

public class GroupBy extends Sort {

    /**
     * Constructor for GroupBy operation.
     * @param numOfBuff The number of buffers available.
     * @param attributeList The attributes to group by.
     */
    public GroupBy(Operator base, int numOfBuff, ArrayList<Attribute> attributeList) {
        super(base, numOfBuff, attributeList);
        this.optype = OpType.GROUPBY;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        int numOfBuff = BufferManager.getBuffersPerJoin();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (Attribute attribute : attributes) {
            newattr.add((Attribute) attribute.clone());
        }
        GroupBy newGroupBy = new GroupBy(newbase, numOfBuff, newattr);
        newGroupBy.setSchema((Schema) newbase.getSchema().clone());
        return newGroupBy;
    }

}