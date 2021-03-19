/**
 * Sort Merge join algorithm
 **/


package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
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

    /**
     * The constructor for SortMergeJoin operator
     * @param join
     */
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
            System.err.println("Page Size must be larger than Tuple Size in join operation");
            return false;
        }

        // Sort the 2 relations
        leftSort = new Sort(left, numBuff, leftAttributeIndex);
        rightSort = new Sort(right, numBuff, rightAttributeIndex);

        return leftSort.open() && rightSort.open();
    }

    /**
     * from sorted tuples select the tuples satisfying the join conditions
     * * And returns a page of output tuples
     **/
    @Override
    public Batch next() {

        Batch joinBatch = findMatchingTuples();
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


    /**
     * from input buffers selects the tuples satisfying join condition
     **/
    private Batch findMatchingTuples() {

        Batch joinBatch = new Batch(batchNum);
        System.out.println("Batch num is " +batchNum);

        Batch leftBatch = leftSort.next();
        Batch rightBatch = rightSort.next();

        if (leftBatch == null || rightBatch == null) {
            return joinBatch;
        }

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


        while (leftTuple != null && rightTuple != null) {

            int compare = 0;
            int conditionPos = 0;
            for (int i = 0; i < leftindex.size(); i++) {
                int leftSingleIndex = leftindex.get(i);
                int rightSingleIndex = rightindex.get(i);
                //get the minimum value of the 2 for their 2 sorting keys
                compare = Tuple.compareTuples(leftTuple, rightTuple, leftSingleIndex, rightSingleIndex);
                if (compare == 0) {
                    conditionPos++;
                } else {
                    break;
                }
            }

            Tuple leftComparatorTuple = null;
            Tuple rightComparatorTuple = null;
            //Comparators to stop the loop
            if (compare == 0) {
                leftComparatorTuple = leftTuple;
                rightComparatorTuple = rightTuple;
            } else if (compare < 0) {
                leftComparatorTuple = leftTuple;
            } else {
                rightComparatorTuple = rightTuple;
            }

            //Comparators to proceed the loop on the eliminating table, assuming the null is passed
            ArrayList<Integer> leftAttributeIndexes = new ArrayList<>();
            ArrayList<Integer> rightAttributeIndexes = new ArrayList<>();
            if (compare == 0) {
                //Take the entire indices
                leftAttributeIndexes = leftindex;
                rightAttributeIndexes = rightindex;
            } else {
                //Include the one it stopped at
                for (int i = 0; i <= conditionPos; i++) {
                    leftAttributeIndexes.add(leftindex.get(i));
                }

                for (int i = 0; i <= conditionPos; i++) {
                    rightAttributeIndexes.add(rightindex.get(i));
                }
            }

            //Removing of tuples based on the stopping attributes for each table if applicable
            while (leftComparatorTuple != null && leftTuple != null
                    && Sort.compareTuples(leftComparatorTuple, leftTuple, leftAttributeIndexes) == 0) {
                leftSet.add(leftTuple);
                leftTuple = leftTuples.pollFirst();
            }

            //Removing of tuples based on the stopping attributes for each table if applicable
            while (rightComparatorTuple != null && rightTuple != null
                    && Sort.compareTuples(rightComparatorTuple, rightTuple, rightAttributeIndexes) == 0) {
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

    // Debugging
    private void printTuple(Tuple t) {
        System.out.print("(");
        System.out.print(t.dataAt(0) + " ");
        System.out.print(t.dataAt(1) + " ");
        System.out.print(t.dataAt(2) + " ");
        System.out.print(t.dataAt(3) + " ");


        System.out.println(")");
    }

}
