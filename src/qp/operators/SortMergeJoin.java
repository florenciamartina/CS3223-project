/**
 * Sort Merge join algorithm
 **/


package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.SortedRun;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

public class SortMergeJoin extends Join {

	private Sort leftSort;
	private Sort rightSort;

    private int batchNum;

    private ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    private ArrayList<Integer> rightindex;  // Indices of the join attributes in right table

	private ArrayList<Attribute> leftAttributeIndex;   //To support join
	private ArrayList<Attribute> rightAttributeIndex;  //To support join


	public SortMergeJoin(Join join) {
    	super(join.getLeft(), join.getRight(), join.getConditionList(), join.getOpType());
        schema = join.getSchema();
        jointype = join.getJoinType();
        numBuff = join.getNumBuff();
    }

    @Override
    public boolean open() {
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();

        leftAttributeIndex = new ArrayList<>();
        rightAttributeIndex = new ArrayList<>();

        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
            leftAttributeIndex.add(leftattr);
            rightAttributeIndex.add(rightattr);
        }

        // Find the batch size
        int tupleSize = getSchema().getTupleSize();
        batchNum = Batch.getPageSize() / tupleSize;



        if (batchNum < 1) {
            System.err.println(" Page Size must be larger than TupleSize in join operation");
            return false;
        }

        // Sort the 2 relations
		leftSort = new Sort(left, numBuff, leftAttributeIndex);
		rightSort = new Sort(right, numBuff, rightAttributeIndex);

        if (!(leftSort.open() && rightSort.open())) {
            return false;
        } else {
        	return true;
        }
    }

    /**
     * * And returns a page of output tuples
     **/
    @Override
    public Batch next() {

        //debug
        System.err.println("Calling SortMergeJoin next() method");
        //debug

    	Batch joinBatch = findMatch();
    	if (!joinBatch.isEmpty()) {
    		return joinBatch;
    	} else {
    		return null;
    	}
    }

    /**
     * Close the operator
     */
    @Override
    public boolean close() {
    	return leftSort.close() && rightSort.close();
    }

    //TODO: Stackoverflow just yolo [139 results]
    /**
     * from input buffers selects the tuples satisfying join condition
     **/
    private Batch findMatch() {
//        Batch joinBatch = new Batch(batchNum);
        int HARD_CODED = 100000;
        Batch joinBatch = new Batch(HARD_CODED);

        Batch leftBatch = leftSort.next();
        Batch rightBatch = rightSort.next();

        //defensive in case nullpointer
        if (leftBatch == null || rightBatch == null) {
            return joinBatch;
        }

        //TODO: Assume we're able to add all tuples into main memory
        ArrayList<Tuple> leftFetch = new ArrayList<>();
        ArrayList<Tuple> rightFetch = new ArrayList<>();

        while (leftBatch != null) {
            leftFetch.addAll(leftBatch.getTuples());
            leftBatch = leftSort.next();
        }

        while (rightBatch != null) {
            rightFetch.addAll(rightBatch.getTuples());
            rightBatch = rightSort.next();
        }


        Deque<Tuple> leftTuples = new ArrayDeque<>(leftFetch);
        Deque<Tuple> rightTuples = new ArrayDeque<>(rightFetch);

        Tuple leftTuple = leftTuples.pollFirst();
        Tuple rightTuple = rightTuples.pollFirst();

        Set<Tuple> leftSet = new HashSet<>();
        Set<Tuple> rightSet = new HashSet<>();




        //TODO: Assume one condition first
        int leftSingleIndex = leftindex.get(0);
        int rightSingleIndex = rightindex.get(0);


        while (leftTuple != null && rightTuple != null) {

            //get the minimum value of the 2 for their 2 sorting keys
            int compare = Tuple.compareTuples(leftTuple, rightTuple, leftSingleIndex, rightSingleIndex);
            Object minimumKey;
            if (compare < 0) {
                //Take left one as it is smaller
                minimumKey = leftTuple.dataAt(leftSingleIndex);
            } else {
                minimumKey = rightTuple.dataAt(rightSingleIndex);
            }

            while (leftTuple != null && compareWithMinimumKey(leftTuple, leftSingleIndex, minimumKey) == 0) {
                leftSet.add(leftTuple);
                leftTuple = leftTuples.pollFirst();
            }

            while (rightTuple != null && compareWithMinimumKey(rightTuple, rightSingleIndex, minimumKey) == 0) {
                rightSet.add(rightTuple);
                rightTuple = rightTuples.pollFirst();
            }

            join(leftSet, rightSet, joinBatch);
            leftSet.clear();
            rightSet.clear();
        }

        return joinBatch;
    }


    private void join(Set<Tuple> leftSet, Set<Tuple> rightSet, Batch joinBatch) {
    	for (Tuple leftTuple : leftSet) {
    		for (Tuple rightTuple : rightSet) {
	        	Tuple joinTuple = leftTuple.joinWith(rightTuple);
	            joinBatch.add(joinTuple);
    		}
    	}
    }


    //Compares a Tuple with a value of the joining attribute
    public static int compareWithMinimumKey(Tuple tuple, int tupleIndex, Object minimumKey) {
        Object tupleData = tuple.dataAt(tupleIndex);
        if (tupleData instanceof Integer) {
            return ((Integer) tupleData).compareTo((Integer) minimumKey);
        } else if (tupleData instanceof String) {
            return ((String) tupleData).compareTo((String) minimumKey);
        } else if (tupleData instanceof Float) {
            return ((Float) tupleData).compareTo((Float) minimumKey);
        } else {
            System.out.println("Tuple: Unknown comparison of the tuples");
            System.exit(1);
            return 0;
        }
    }



    // Debugging
    private void printTuple(Tuple t)  {
        System.out.print("(");
        System.out.print(t.dataAt(0) + " ");
        System.out.print(t.dataAt(1)+ " ");
        System.out.print(t.dataAt(2)+ " ");
        System.out.print(t.dataAt(3)+ " ");


        System.out.println(")");
    }



}
