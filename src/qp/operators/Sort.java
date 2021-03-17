/**
 * ExternalSort Operation
 **/

package qp.operators;

import java.io.*;
import java.util.ArrayList;

import qp.optimizer.BufferManager;
import qp.utils.*;

public class Sort extends Operator {

    int batchSize;  // Number of tuples per outbatch
    Operator base;
    ArrayList<Integer> attributeIndexes;
    ArrayList<Attribute> attributes;
    TupleReader tupleReader;
    TupleWriter tupleWriter;

    /**
     * The following fields are required during
     * * execution of the Sort operator
     **/
    boolean eos;                // Indicate whether end of stream is reached or not
    Batch outBatch;             // This is the current output buffer

    // Sorting attributes
    boolean isAsc;
    boolean isDistinct;

    int numOfBuff;
    int numSortedRuns;
    int numOfPasses = 0;
    ArrayList<Tuple> sorted;
    int totalSize = 0;          // debugging purposes
    int readSize = 0;           // debugging purposes

    /**
     * constructor
     **/
    public Sort(Operator base, int numOfBuff, ArrayList<Attribute> attributeList, boolean isAsc, boolean isDistinct) {
        super(OpType.SORT);
        this.base = base;
        this.schema = base.schema;
        this.numOfBuff = numOfBuff;
        this.attributes = attributeList;
        this.isAsc = isAsc;
        this.isDistinct = isDistinct;
    }

    public Sort(Operator base, int numOfBuff, ArrayList<Attribute> attributeList) {
        this(base, numOfBuff, attributeList, true, false);
    }

    public boolean open() {
        // Base is to be materialized for Sort to perform
        if (!base.open()) {
            return false;
        }

        eos = false;  // Since the stream is just opened

        // Select the number of tuples per batch
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        if (batchSize <= 0) {
            System.err.println("Page size must be larger than Tuple size in join operation");
            return false;
        }

        // Find the index for each attribute
        this.attributeIndexes = new ArrayList<>();
        for (Attribute a : attributes) {
            attributeIndexes.add(schema.indexOf(a));
        }

        numSortedRuns = generateSortedRuns();

        if (numSortedRuns == 0) {
            eos = true;
            return true;
        }

        mergeSortedRuns();

        // Read final sorted run
        numOfPasses++;
        String filename = String.format("SortTemp-P%d-SR0", numOfPasses == 1 ? 0 : numOfPasses);
        tupleReader = new TupleReader(filename, batchSize * numSortedRuns);
        tupleReader.open();
        sorted = new ArrayList<>();
        while (!tupleReader.isEOF()) {
            sorted.add(tupleReader.next());
        }
        tupleReader.close();

        if (sorted.size() != totalSize) {
            System.err.println("Error in performing Sort operation");
            System.err.println("Num of read tuples != Num of generated tuples");
            return false;
        }

        // Cleanup
        File f = new File(filename);
        f.delete();

        return true;
    }

    public Batch next() {

        // Close when end of stream
        if (eos) {
            close();
            printStatistics();
            return null;
        }

        /** An output buffer is initiated **/
        outBatch = new Batch(batchSize);

        while (!outBatch.isFull() && !sorted.isEmpty()) {
            outBatch.add(sorted.get(0));
            sorted.remove(0);
        }

        if (sorted.isEmpty()) {
            eos = true;
            return outBatch;
        }

        return outBatch;
    }

    private int generateSortedRuns() {

        int numSortedRuns = 0;
        Batch inputBatch;

        // Generate sorted runs
        while ((inputBatch = base.next()) != null) {
            String filename = String.format("SortTemp-P0-SR%d", numSortedRuns);

            // Tuples in current sorted run
            ArrayList<Tuple> tuples = new ArrayList<>();

            // Read-in tuples
            for (int i = 0; i < numOfBuff; i++) {
                tuples.addAll(inputBatch.getTuples());

                if (i < numOfBuff - 1) {
                    inputBatch = base.next();
                    if (inputBatch == null) {
                        break;
                    }
                }
            }

            // Sort the tuples and generate sorted run
            if (isAsc) {
                tuples.sort((x, y) -> compareTuples(x, y, attributeIndexes));
            } else {
                tuples.sort((x, y) -> compareTuples(y, x, attributeIndexes));
            }

            // Write sorted runs
            tupleWriter = new TupleWriter(filename, batchSize);
            tupleWriter.open();
            for (Tuple t : tuples) {
                tupleWriter.next(t);
                totalSize++;
            }
            tupleWriter.close();

            numSortedRuns++;
        }

        return numSortedRuns;
    }

    private void mergeSortedRuns() {

        int totalSRs = this.numSortedRuns;
        int numInputBuffers = numOfBuff - 1;

        int readSRs = 0;
        int writeSRs = 0;
        int genSRs = numSortedRuns;

//        int writeSize = 0;

        boolean updatePass = false;
        while (totalSRs > 1) {

            // Setup input buffers
            ArrayList<Batch> inputBuffers = new ArrayList<>();

            ArrayList<TupleReader> tupleReaders = new ArrayList<>();
            int numMergedSRs = Math.min(numInputBuffers, genSRs);
            for (int i = 0; i < numMergedSRs; i++) {

                if (genSRs <= readSRs + i) {
                    updatePass = true;
                    break;
                }

                String sortedRunFile = String.format("SortTemp-P%d-SR%d", numOfPasses, readSRs + i);
                TupleReader tr = new TupleReader(sortedRunFile, batchSize);
                tr.open();
                tupleReaders.add(tr);
            }

            numMergedSRs = tupleReaders.size();
            readSRs += numMergedSRs;

            // Read input buffers
            for (TupleReader tr : tupleReaders) {
                Batch b = new Batch(batchSize);
                while (!b.isFull() && !tr.isEOF()) {
                    b.add(tr.next());
                    readSize++;
                }

                if (tr.isEOF()) {
                    tr.close();
                }

                inputBuffers.add(b);
            }

            // Find selected buffer to get tuple and write to output
            String fileOutput = String.format("SortTemp-P%d-SR%d", numOfPasses + 1, writeSRs);
            tupleWriter = new TupleWriter(fileOutput, batchSize);
            tupleWriter.open();

            Tuple prevTuple = null;
            while (!inputBuffers.stream().allMatch(Batch::isEmpty)) {
                Tuple currTuple = getTuple(inputBuffers, tupleReaders);
                if (!isDistinct || prevTuple == null || isDistinct(prevTuple, currTuple)) {
                    tupleWriter.next(currTuple);
//                    writeSize++;
                }

                prevTuple = currTuple;

                for (int i = 0; i < numMergedSRs; i++) {
                    Batch inputBuffer = inputBuffers.get(i);
                    TupleReader tr = tupleReaders.get(i);

                    if (!inputBuffer.isEmpty() || tr.isEOF()) {
                        continue;
                    }

                    while (!inputBuffer.isFull() && !tr.isEOF()) {
                        inputBuffer.add(tr.next());
//                        readSize++;
                    }
                }
            }

            tupleWriter.close();

            totalSRs -= numMergedSRs;
            writeSRs++;
            totalSRs++;

            if (updatePass) {
//                System.out.printf("Read SRs #%d: %d\n", numOfPasses + 1, readSRs);
                genSRs = writeSRs;
                readSRs = 0;
                writeSRs = 0;
                numOfPasses++;
//                System.out.printf("Read size #%d: %d\n", numOfPasses, readSize);
//                System.out.printf("Write size #%d: %d\n", numOfPasses, writeSize);
//                readSize = 0;
//                writeSize = 0;
                updatePass = false;
            }
        }
    }

    private Tuple getTuple(ArrayList<Batch> inputBuffers, ArrayList<TupleReader> tupleReaders) {

        Tuple selectedTuple = null;
        Batch selectedBatch = null;
        int compareResult;

        for (int i = 0; i < inputBuffers.size(); i++) {

            Batch inputBuffer = inputBuffers.get(i);

            if (inputBuffer.isEmpty()) {
                TupleReader tr = tupleReaders.get(i);
                while (!tr.isEOF() && !inputBuffer.isFull()) {
                    inputBuffer.add(tr.next());
                    readSize++;
                }
            }

            if (inputBuffer.isEmpty()) continue;

            Tuple currTuple = inputBuffer.peek();

            if (selectedTuple == null) {
                selectedTuple = currTuple;
                selectedBatch = inputBuffer;
                continue;
            }

            compareResult = compareTuples(currTuple, selectedTuple, attributeIndexes);

            if ((isAsc && compareResult >= 0) || (!isAsc && compareResult <= 0)) {
                continue;
            }

            selectedTuple = currTuple;
            selectedBatch = inputBuffer;
        }

        assert selectedBatch != null;

        return selectedBatch.poll();
    }

    public boolean close() {

        // Remove temp files
        System.gc();
        for (int i = 0; i <= numOfPasses; i++) {
            int j = 0;
            while (true) {
                String filename = String.format("SortTemp-P%d-SR%d", i, j);
                File f = new File(filename);

                if (!f.exists()) break;

                f.delete();
                j++;
            }
        }

        return true;
    }


    public Object clone() {
        Operator newBase = (Operator) base.clone();
        int numOfBuff = BufferManager.getBuffersPerJoin();
        ArrayList<Attribute> newAttr = new ArrayList<>();
        for (Attribute attribute : attributes) {
            newAttr.add((Attribute) attribute.clone());
        }
        Sort newSorter = new Sort(newBase, numOfBuff, newAttr);
        newSorter.setSchema((Schema) newBase.getSchema().clone());
        return newSorter;
    }

    /**
     * Compare tuples based on the given attributes
     */
    public static int compareTuples(Tuple t1, Tuple t2, ArrayList<Integer> attributeIndexes) {
        int result = 0;
        for (int index : attributeIndexes) {
            result = Tuple.compareTuples(t1, t2, index);
            if (result != 0) {
                break;
            }
        }

        return result;
    }

    /**
     * To check whether the current tuple is already present
     **/
    private boolean isDistinct(Tuple tuple1, Tuple tuple2) {

        if (attributeIndexes.isEmpty()) {
            return !tuple1.equals(tuple2);
        }

        int result;
        for (Integer i : attributeIndexes) {
            result = Tuple.compareTuples(tuple1, tuple2, i);

            if (result != 0) {
                return true;
            }
        }
        return false;
    }

    public Operator getBase() {
        return this.base;
    }

    // Debugging
    private void printStatistics() {
        System.out.println("Sorted Runs: " + numSortedRuns);
        System.out.println("Passes: " + numOfPasses);
        System.out.printf("Total tuples: %d\n", totalSize);
    }
}