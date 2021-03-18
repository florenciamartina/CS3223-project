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

    int leftBlockCurs;                   // Cursor for left side block
    int rightCurs;                  // Cursor for right side buffer
    int leftCurs;              // Cursor for left side buffer page
    boolean eosLeft;                // Whether end of stream (left table) is reached
    boolean eosRight;               // Whether end of stream (right table) is reached

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
        /** select number of tuples per batch **/
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        /** find indices attributes of join conditions **/
        leftIndex = new ArrayList<>();
        rightIndex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftAttr = con.getLhs();
            Attribute rightAttr = (Attribute) con.getRhs();
            leftIndex.add(left.getSchema().indexOf(leftAttr));
            rightIndex.add(right.getSchema().indexOf(rightAttr));
        }
        Batch rightPage;

        /** initialize the cursors of input buffers **/
        leftBlockCurs = 0;
        leftCurs = 0;
        rightCurs = 0;
        eosLeft = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosRight = true;

        /** Right hand side table is to be materialized
         ** for the Block Nested Loop join to perform
         **/
        if (!right.open()) {
            return false;
        }

        /** If the right operator is not a base table then
         ** Materialize the intermediate result from right
         ** into a file
         **/
        fileNum++;
        rfName = "BNLJtemp-" + String.valueOf(fileNum);
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfName));
            while ((rightPage = right.next()) != null) {
                out.writeObject(rightPage);
            }
            out.close();
        } catch (IOException io) {
            System.out.println("BlockNestedLoopJoin: Error writing to temporary file");
            return false;
        }

        if (!right.close())
            return false;

        return left.open();
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        int i, j, k;

        if (eosLeft) {
            return null;
        }

        outBatch = new Batch(batchSize);

        while (!outBatch.isFull()) {
            if (leftBlockCurs == 0 && eosRight) {

                /** new left block is to be fetched **/
                leftBlock = new ArrayList<>();
                for (int b = 0; b < getNumBuff() - 2; b++) {
                    Batch leftBatch = left.next();
                    if (leftBatch == null) break;
                    leftBlock.add(leftBatch);
                }

                if (leftBlock.isEmpty()) {
                    eosLeft = true;
                    return outBatch;
                }

                /** Whenever a new left block came, we have to start the
                 ** scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfName));
                    eosRight = false;
                } catch (IOException io) {
                    System.err.println("BlockNestedLoopJoin:error in reading the file");
                    System.exit(1);
                }

            }

            while (!eosRight) {
                try {
                    if (rightCurs == 0 && leftCurs == 0) {
                        rightBatch = (Batch) in.readObject();
                    }

                    for (i = leftBlockCurs; i < leftBlock.size(); ++i) {
                        Batch leftBatch = leftBlock.get(i);
                        for (j = leftCurs; j < leftBatch.size(); ++j) {
                            for (k = rightCurs; k < rightBatch.size(); ++k) {
                                Tuple leftTuple = leftBatch.get(j);
                                Tuple rightTuple = rightBatch.get(k);
                                if (leftTuple.checkJoin(rightTuple, leftIndex, rightIndex)) {
                                    Tuple outTuple = leftTuple.joinWith(rightTuple);
                                    outBatch.add(outTuple);
                                    if (outBatch.isFull()) {
                                        boolean isEndLeftBlock = i == leftBlock.size() - 1;
                                        boolean isEndLeftBatch = j == leftBatch.size() - 1;
                                        boolean isEndRightBatch = k == rightBatch.size() - 1;

                                        leftBlockCurs = isEndLeftBlock && isEndLeftBatch && isEndRightBatch
                                                ? 0 : !isEndLeftBlock && isEndLeftBatch && isEndRightBatch
                                                ? i + 1 : i;

                                        leftCurs = isEndLeftBatch && isEndRightBatch
                                                ? 0
                                                : !isEndLeftBatch && isEndRightBatch
                                                ? j + 1 : j;

                                        rightCurs = isEndRightBatch ? 0 : k + 1;

                                        return outBatch;
                                    }
                                }
                            }
                            rightCurs = 0;
                        }
                        leftCurs = 0;
                    }
                    leftBlockCurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedLoopJoin: Error in reading temporary file");
                    }
                    eosRight = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedLoopJoin: Error in deserializing temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedLoopJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outBatch;
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
