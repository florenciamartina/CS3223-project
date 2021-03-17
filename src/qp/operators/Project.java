/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
    int maxValue = Integer.MIN_VALUE;
    int minValue = Integer.MAX_VALUE;
    int count = 0;
    double average = 0;
    int sum = 0;
    boolean isAggregate = false;

    /**
     * The following fields are required during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;
    int[] aggregateIdx;
    Object[] aggregateData;

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        aggregateIdx = new int[attrset.size()];
        aggregateData = new Object[attrset.size()];

//        for (int i = 0; i < attrset.size(); ++i) {
//            Attribute attr = attrset.get(i);

//            if (attr.getAggType() != Attribute.NONE) {
//                System.err.println("Aggregation is not implemented.");
//                System.exit(1);
//            }

        checkAggregate(baseSchema);

//            int index = baseSchema.indexOf(attr.getBaseAttribute());
//            attrIndex[i] = index;
//        }
        return true;
    }

    private void checkAggregate(Schema baseSchema) {
        int index = 0;
        int aggType = Attribute.NONE;
        for (int i = 0; i < attrset.size(); i++) {
            Attribute attribute = attrset.get(i);

            index = baseSchema.indexOf(attribute.getBaseAttribute());
            aggType = attribute.getAggType();
            this.attrIndex[i] = index;
            this.aggregateIdx[i] = aggType;

            switch(aggType) {
            case Attribute.MAX:
                this.aggregateData[i] = (Object) maxValue;
                break;
            case Attribute.MIN:
                this.aggregateData[i] = (Object) minValue;
                break;
            case Attribute.COUNT:
                this.aggregateData[i] = (Object) count;
                break;
            case Attribute.SUM:
                this.aggregateData[i] = (Object) sum;
                break;
            case Attribute.AVG:
                this.aggregateData[i] = (Object) average;
                break;
            default:
                break;
            }
        }
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        inbatch = base.next();

        if (isAggregate) {
            if (inbatch == null) {
                ArrayList<Object> aggregateResult = new ArrayList<>();
                for (Object aggData : aggregateData) {
                    aggregateResult.add(aggData);
                }

                Tuple aggregateTuple = new Tuple(aggregateResult);
                outbatch.add(aggregateTuple);
                isAggregate = false;

                return outbatch;
            }
        }

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

                if (aggregateIdx[j] == Attribute.NONE) {
                    present.add(data);

                } else if (aggregateIdx[j] == Attribute.MAX) {
                    isAggregate = true;
                    try {
                        if ((int) data > (int) aggregateData[j]) {
                            aggregateData[j] = data;
                        }
                    } catch (Exception e) {
                        System.out.println("MAX only accepts integer attribute!");
                        System.exit(1);
                    }
                } else if (aggregateIdx[j] == Attribute.MIN) {
                    isAggregate = true;
                    try {
                        if ((int) data < (int) aggregateData[j]) {
                            aggregateData[j] = data;
                        }
                    } catch (Exception e) {
                        System.out.println("MIN only accepts integer attribute!");
                        System.exit(1);
                    }
                } else if (aggregateIdx[j] == Attribute.SUM) {
                    isAggregate = true;
                    try {
                        aggregateData[j] = (int) aggregateData[j] + (int) data;
                    } catch (Exception e) {
                        System.out.println("SUM only accepts integer attribute!");
                        System.exit(1);
                    }

                } else if (aggregateIdx[j] == Attribute.COUNT) {
                    isAggregate = true;
                    try {
                        aggregateData[j] = (int) aggregateData[j] + 1;
                    } catch (Exception e) {
                        System.out.println("COUNT only accepts integer attribute!");
                        System.exit(1);
                    }
                } else if (aggregateIdx[j] == Attribute.AVG) {
                    isAggregate = true;
                    try {
                        sum = sum + (int) data;
                        count = count + 1;
                        average = 1.0 * sum/count;
                        aggregateData[j] = average;

                    } catch (Exception e) {
                        System.out.println("AVG only accepts integer attribute!");
                        System.exit(1);
                    }
                }
            }

            if (!isAggregate) {
                Tuple outtuple = new Tuple(present);
                outbatch.add(outtuple);
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
