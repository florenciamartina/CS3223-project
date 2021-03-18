package qp.operators;

/**
 * Block Nested Loop Join algorithm
 **/

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedLoopJoin extends Join {
    static int fileNum = 0;         // To get unique fileNum for this operation
    int batchSize;                  // Number of tuples per out batch
    ArrayList<Integer> leftIndex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightIndex;  // Indices of the join attributes in right table
    String rfName;                  // The file name where the right table is materialized
    Batch outBatch;                 // Buffer page for output
    ArrayList<Batch> leftBlock;     // Buffer block for left input stream
    Batch rightBatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int rightCurs;                  // Cursor for right side buffer
    int leftCurs;                   // Cursor for left side buffer page
    boolean eosLeft;                // Whether end of stream (left table) is reached
    boolean eosRight;               // Whether end of stream (right table) is reached

    int numOfTuplesWrite = 0; // debug

    public BlockNestedLoopJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        // Select number of tuples per batch
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        // Find indices attributes of join conditions
        leftIndex = new ArrayList<>();
        rightIndex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftAttr = con.getLhs();
            Attribute rightAttr = (Attribute) con.getRhs();
            leftIndex.add(left.getSchema().indexOf(leftAttr));
            rightIndex.add(right.getSchema().indexOf(rightAttr));
        }
        Batch rightPage;

        // Initialize the cursors of input buffers
        leftCurs = 0;
        rightCurs = 0;
        eosLeft = false;

        // Because right stream is to be repetitively scanned
        // if it reached end, we have to start new scan
        eosRight = true;

        // Right hand side table is to be materialized
        // for the Block Nested Loop join to perform
        if (!right.open()) {
            return false;
        }

        // If the right operator is not a base table then
        // Materialize the intermediate result from right
        // into a file
        fileNum++;
        rfName = "BNLJtemp-" + fileNum;
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfName));
            while ((rightPage = right.next()) != null) {
                out.writeObject(rightPage);
            }
            out.close();
        } catch (IOException io) {
            System.err.println("BlockNestedLoopJoin: Error writing to temporary file");
            return false;
        }

        if (!right.close())
            return false;

        return left.open();
    }

    /**
     * Selects tuples satisfying the join condition from input buffers and returns.
     *
     * @return the next page of output tuples.
     */
    @Override
    public Batch next() {
        int i, j;

        if (eosLeft) {
            System.out.println("#Tuples: " + numOfTuplesWrite); // debug
            close();
            return null;
        }

        outBatch = new Batch(batchSize);
        while (!outBatch.isFull()) {

            if (leftCurs == 0 && eosRight) {
                fetchLeftBlock();

                if (leftBlock.get(0) == null) {
                    eosLeft = true;
                    return outBatch;
                }
            }

            int numOfLeftTuples = getNumLeftTuples();
            int tuplesPerBatch = leftBlock.get(0).size();

            while (!eosRight) {
                try {
                    if (leftCurs == 0 && rightCurs == 0) {
                        rightBatch = (Batch) in.readObject();
                    }

                    for (i = leftCurs; i < numOfLeftTuples; i++) {
                        int leftBatchIndex = i / tuplesPerBatch;
                        int leftTupleIndex = i % tuplesPerBatch;

                        Tuple leftTuple = leftBlock.get(leftBatchIndex).get(leftTupleIndex);
                        for (j = rightCurs; j < rightBatch.size(); j++) {
                            Tuple rightTuple = rightBatch.get(j);

                            if (leftTuple.checkJoin(rightTuple, leftIndex, rightIndex)) {
                                Tuple outTuple = leftTuple.joinWith(rightTuple);
                                outBatch.add(outTuple);
                                numOfTuplesWrite++;

                                if (outBatch.isFull()) {
                                    boolean isEndLeft = i == numOfLeftTuples - 1;
                                    boolean isEndRight = j == rightBatch.size() - 1;

                                    leftCurs = isEndLeft && isEndRight ? 0
                                            : !isEndLeft && isEndRight
                                            ? i + 1 : i;

                                    rightCurs = isEndRight ? 0 : j + 1;

                                    return outBatch;
                                }
                            }
                        }
                        rightCurs = 0;
                    }
                    leftCurs = 0;

                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.err.println("BlockNestedLoopJoin: Error in reading temporary file");
                    }
                    eosRight = true;
                } catch (ClassNotFoundException c) {
                    System.err.println("BlockNestedLoopJoin: Error in deserializing temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.err.println("BlockNestedLoopJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outBatch;
    }

    private void fetchLeftBlock() {
        leftBlock = new ArrayList<>();
        for (int b = 0; b < getNumBuff() - 2; b++) {
            Batch leftBatch = left.next();
            leftBlock.add(leftBatch);
            if (leftBatch == null) break;
        }

        // Whenever a new left block came, we have to start the
        // scanning of right table
        try {
            in = new ObjectInputStream(new FileInputStream(rfName));
            eosRight = false;
        } catch (IOException io) {
            System.err.println("BlockNestedLoopJoin:error in reading the file");
            System.exit(1);
        }
    }

    private int getNumLeftTuples() {
        int numOfLeftTuples = 0;
        for (Batch leftBatch : leftBlock) {
            if (leftBatch == null) break;
            numOfLeftTuples += leftBatch.size();
        }

        return numOfLeftTuples;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfName);
        f.delete();
        return true;
    }
}