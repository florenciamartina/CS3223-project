/**
 * ExternalSort Operation
 **/

package qp.operators;

import java.io.*;
import java.util.ArrayList;
import java.util.UUID;

import qp.optimizer.BufferManager;
import qp.utils.*;

public class Sort extends Operator {

    int batchSize;  // Number of tuples per outbatch
    Operator base;
    ArrayList<Attribute> attributes;
    ArrayList<Integer> attributeIndexes;

    // Input and output
    String sortedFileName;
    TupleReader tupleReader;
    TupleWriter tupleWriter;

    // To avoid conflicts between Sort operations
    String uuid = UUID.randomUUID().toString();

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
    int numOfPasses = 0;
    int totalInputSize = 0;          // debugging purposes
    int totalOutputSize = 0;         // debugging purposes

    // Merging
    int numSortedRuns;
    int maxTuplesInSR = 0;

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
            System.err.println("Page size must be larger than tuple size in Sort operation");
            return false;
        }

        // Find the index for each attribute
        this.attributeIndexes = new ArrayList<>();
        for (Attribute a : attributes) {
            attributeIndexes.add(schema.indexOf(a));
        }

        generateSortedRuns();

        if (numSortedRuns == 0) {
            eos = true;
            return true;
        }

        mergeSortedRuns();

        return true;
    }

    public Batch next() {

        // Close when end of stream
        if (eos) {
            close();
            printStatistics(); // debug
            return null;
        }

        /** An output buffer is initiated **/
        outBatch = new Batch(batchSize);

        // Read from final sorted file
        while (!outBatch.isFull() && !tupleReader.isEOF()) {
            outBatch.add(tupleReader.next());
            totalOutputSize++;
        }

        if (tupleReader.isEOF()) {
            eos = true;
            return outBatch;
        }

        return outBatch;
    }

    private void generateSortedRuns() {

        numSortedRuns = 0;
        Batch inputBatch;

        // Generate sorted runs
        while ((inputBatch = base.next()) != null) {
            String filename = getFileName(0, numSortedRuns);

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

            // Sort the tuples
            tuples.sort(isAsc
                    ? (x, y) -> compareTuples(x, y, attributeIndexes)
                    : (x, y) -> compareTuples(y, x, attributeIndexes));

            // Write sorted runs
            tupleWriter = new TupleWriter(filename, batchSize);
            tupleWriter.open();
            for (Tuple t : tuples) {
                tupleWriter.next(t);
                totalInputSize++;
            }
            tupleWriter.close();

            maxTuplesInSR = Math.max(maxTuplesInSR, tuples.size());

            numSortedRuns++;
        }
    }

    private void mergeSortedRuns() {

        int numOfInputBuffers = numOfBuff - 1;

        while (numSortedRuns > 1) {

            int mergedSRs = 0;
            int currSRs = numSortedRuns;

            numSortedRuns = 0;
            while (mergedSRs < currSRs) {
                ArrayList<String> fileInputs = new ArrayList<>();
                for (int i = 0; i < numOfInputBuffers; i++) {
                    if (mergedSRs == currSRs) {
                        break;
                    }

                    String fileInput = getFileName(numOfPasses, mergedSRs);
                    fileInputs.add(fileInput);
                    mergedSRs++;
                }

                mergeRuns(fileInputs);
                numSortedRuns++;

                // Cleanup temp files
                for (String filename : fileInputs) {
                    File f = new File(filename);
                    f.delete();
                }
            }

            numOfPasses++;
        }

        sortedFileName = getFileName(numOfPasses, 0);
        tupleReader = new TupleReader(sortedFileName, batchSize);
        tupleReader.open();
    }

    private void mergeRuns(ArrayList<String> fileInputs) {

        // Setup input buffers and readers
        ArrayList<Batch> inputBuffers = new ArrayList<>();
        ArrayList<TupleReader> tupleReaders = new ArrayList<>();

        for (String fileInput : fileInputs) {
            TupleReader tupleReader = new TupleReader(fileInput, batchSize);
            tupleReaders.add(tupleReader);
            tupleReader.open();

            Batch inputBuffer = new Batch(batchSize);
            while (!inputBuffer.isFull() && !tupleReader.isEOF()) {
                inputBuffer.add(tupleReader.next());
            }

            inputBuffers.add(inputBuffer);
        }

        // Merge input buffers
        mergeTuples(inputBuffers, tupleReaders);

    }

    private void mergeTuples(ArrayList<Batch> inputBuffers, ArrayList<TupleReader> tupleReaders) {

        String fileOutput = getFileName(numOfPasses + 1, numSortedRuns);
        TupleWriter tw = new TupleWriter(fileOutput, maxTuplesInSR);
        tw.open();

        Batch outBatch = new Batch(batchSize);

        int tuplesInSR = 0;

        Tuple prevTuple = null;
        while (!inputBuffers.stream().allMatch(Batch::isEmpty)) {
            Tuple selectedTuple = getSelectedTuple(inputBuffers, tupleReaders);

            if (!isDistinct || prevTuple == null || isDistinct(prevTuple, selectedTuple)) {
                outBatch.add(selectedTuple);

                if (outBatch.isFull()) {
                    while (!outBatch.isEmpty()) {
                        tw.next(outBatch.poll());
                        tuplesInSR++;
                    }
                }
            }

            prevTuple = selectedTuple;
        }

        while (!outBatch.isEmpty()) {
            tw.next(outBatch.poll());
            tuplesInSR++;
        }

        tw.close();

        maxTuplesInSR = Math.max(tuplesInSR, maxTuplesInSR);
    }

    private Tuple getSelectedTuple(ArrayList<Batch> inputBuffers, ArrayList<TupleReader> tupleReaders) {

        Tuple selectedTuple = null;
        int selected = -1;

        int compareResult;
        for (int i = 0; i < inputBuffers.size(); i++) {

            Batch inputBuffer = inputBuffers.get(i);

            if (inputBuffer.isEmpty()) {
                continue;
            }

            Tuple currTuple = inputBuffer.peek();

            if (selectedTuple == null) {
                selectedTuple = currTuple;
                selected = i;
                continue;
            }

            compareResult = compareTuples(currTuple, selectedTuple, attributeIndexes);

            if ((isAsc && compareResult >= 0) || (!isAsc && compareResult <= 0)) {
                continue;
            }

            selectedTuple = currTuple;
            selected = i;
        }

        Batch selectedBatch = inputBuffers.get(selected);
        TupleReader tr = tupleReaders.get(selected);
        selectedTuple = selectedBatch.poll();

        // Refill buffer
        if (!tr.isEOF()) {
            selectedBatch.add(tr.next());
        } else {
            tr.close();
        }

        return selectedTuple;
    }

    private String getFileName(int numOfPasses, int sortedRunIndex) {
        return String.format("SortTemp-%s-P%d-SR%d", uuid, numOfPasses, sortedRunIndex);
    }

    public boolean close() {
        // Close streams
        tupleReader.close();
        tupleWriter.close();

        // Remove temp files
        File f = new File(sortedFileName);
        f.delete();

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
        System.out.println("Passes: " + numOfPasses);
        System.out.printf("Input tuples: %d\n", totalInputSize);
        System.out.printf("Output tuples: %d\n", totalOutputSize);
    }
}