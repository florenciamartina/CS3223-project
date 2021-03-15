/**
 * ExternalSort Operation
 **/

package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.SortedRun;
import qp.utils.Tuple;

public class Sort extends Operator {

    int batchSize;  // Number of tuples per outbatch
    Operator base;
    ArrayList<Integer> attributeIndexes;
    ArrayList<Attribute> attributes;
    ObjectInputStream in;       // Input file being scanned
    ObjectOutputStream out;     // Output file being written

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
    SortedRun sorted;
    ArrayList<SortedRun> sortedRuns;
    int totalSize = 0;          // debugging purposes

    /**
     * constructor
     **/
    public Sort(Operator base, int numOfBuff, ArrayList<Attribute> attributeList, boolean isAsc, boolean isDistinct) {
        super(OpType.EXTERNALSORT);
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

        System.out.println("ascending" + this.isAsc);

        // Base is to be materialized for Sort to perform
        if (!base.open()) {
            return false;
        }

        eos = false;  // Since the stream is just opened

        // Select the number of tuples per batch
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

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
        try {
            numOfPasses++;
            System.out.println("Sorted Runs:" + numSortedRuns);
            System.out.println("Passes: " + numOfPasses);
            String filename = String.format("SortTemp-P%d", numOfPasses == 1 ? 0 : numOfPasses);
            in = new ObjectInputStream(new FileInputStream(filename));
            sorted = (SortedRun) in.readObject();
            System.out.println("SortedSize: " + sorted.size());

        } catch (ClassNotFoundException cnf) {
            System.err.println("Sort: Class not found");
        } catch (IOException io) {
            System.err.println("Sort: Error in reading from temporary file");
        }

        return true;
    }

    public Batch next() {

        // Close when end of stream
        if (eos) {
            close();
            System.out.println(totalSize);
            return null;
        }

        /** An output buffer is initiated **/
        outBatch = new Batch(batchSize);

        while (!outBatch.isFull() && !sorted.isEmpty()) {
            outBatch.add(sorted.poll());
            totalSize++;
        }

        if (sorted.isEmpty()) {
            eos = true;
            return outBatch;
        }

        return outBatch;
    }

    private int generateSortedRuns() {

        int numSortedRun = 0;
        Batch inputBatch;

        sortedRuns = new ArrayList<>();

        try {
            String filename = "SortTemp-P0";
            out = new ObjectOutputStream(new FileOutputStream(filename));

            // Generate sorted runs
            while ((inputBatch = base.next()) != null) {

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
                SortedRun sr = isAsc
                    ? new SortedRun(tuples, attributeIndexes)
                    : new SortedRun(tuples, attributeIndexes, false);
                sortedRuns.add(sr);

                // Write sorted runs
                out.writeObject(sr);

                numSortedRun++;
            }

            out.close();

        } catch (IOException io) {
            System.out.println("Sort: Error writing to temporary file.");
        }

        return numSortedRun;
    }

    private void mergeSortedRuns() {
        try {
            String fileOutput = String.format("SortTemp-P%d", numOfPasses + 1);
            out = new ObjectOutputStream(new FileOutputStream(fileOutput));

            String fileInput = String.format("SortTemp-P%d", numOfPasses);
            in = new ObjectInputStream(new FileInputStream(fileInput));

            boolean updatePass = false;
            SortedRun merged = null;
            while (numSortedRuns > 1) {
                System.out.println("Start loop pass #" + numOfPasses);

                // Read in sorted runs
                ArrayList<SortedRun> mergedSortedRuns = new ArrayList<>();
                int numOfMergedSortedRuns = Math.min(numOfBuff - 1, numSortedRuns);
                SortedRun sr;
                for (int i = 0; i < numOfMergedSortedRuns; i++) {
                    try {
                        sr = (SortedRun) in.readObject();
                        mergedSortedRuns.add(sr);
                    } catch (EOFException eof) {
                        updatePass = true;
                        break;
                    }
                }

                // Merge sorted runs
                Batch mergeOutput = new Batch(batchSize);
                SortedRun selectedSortedRun;

                Tuple prevTuple = null;
                Tuple currTuple;
                while (!mergedSortedRuns.stream().allMatch(SortedRun::isEmpty)) {
                    selectedSortedRun = findSelectedSortedRun(mergedSortedRuns, mergedSortedRuns.size(), isAsc);

                    currTuple = selectedSortedRun.poll();
                    if (!isDistinct || prevTuple == null || isDistinct(prevTuple, currTuple)) {
                        mergeOutput.add(currTuple);
                    }

                    prevTuple = currTuple;
//                    mergeOutput.add(selectedSortedRun.poll());
                }

                sortedRuns.removeIf(SortedRun::isEmpty);
                merged = new SortedRun(mergeOutput.getTuples());
                sortedRuns.add(merged);

                numSortedRuns -= mergedSortedRuns.size();

                // Write merged sorted runs
                numSortedRuns++;
                out.writeObject(merged);
                System.out.print("Merged: ");
                printSortedRun(merged);

                if (updatePass) {
                    numOfPasses++;

                    fileOutput = String.format("SortTemp-P%d", numOfPasses + 1);
                    out = new ObjectOutputStream(new FileOutputStream(fileOutput));

                    fileInput = String.format("SortTemp-P%d", numOfPasses);
                    in = new ObjectInputStream(new FileInputStream(fileInput));

                    updatePass = false;
                }
            }

            in.close();
            out.close();

        } catch (ClassNotFoundException cnf) {
            System.err.println("Sort: Class not found");
        } catch (IOException io) {
            System.err.println("Sort: Error in reading sorted runs");
        }

    }

    private SortedRun findSelectedSortedRun(ArrayList<SortedRun> sortedRuns, int numOfMergedSortedRuns, boolean isAsc) {
        Tuple selectedTuple = null;
        SortedRun selectedSortedRun = null;
        int compareResult;
        for (int i = 0; i < numOfMergedSortedRuns; i++) {

            if (sortedRuns.get(i).isEmpty()) {
                continue;
            }

            Tuple currTuple = sortedRuns.get(i).peek();

            if (selectedTuple == null) {
                selectedTuple = currTuple;
                selectedSortedRun = sortedRuns.get(i);
            }

            compareResult = SortedRun.compareTuples(currTuple, selectedTuple, attributeIndexes);

            if ((isAsc && compareResult >= 0) || (!isAsc && compareResult <= 0)) {
                continue;
            }

            selectedTuple = currTuple;
            selectedSortedRun = sortedRuns.get(i);
        }

        return selectedSortedRun;
    }

    public boolean close() {

        // Close streams
        try {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
        } catch (IOException io) {
            System.err.println("Sort: Failed to close streams");
        }

        // Remove temp files
        System.gc();
        for (int i = 0; i <= numOfPasses; i++) {
            String filename = String.format("SortTemp-P%d", i);
            File f = new File(filename);
            f.delete();

        }

        return true;
    }


    public Object clone() {
        Operator newbase = (Operator) base.clone();
        int numOfBuff = BufferManager.getBuffersPerJoin();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attributes.size(); ++i) {
            newattr.add((Attribute) attributes.get(i).clone());
        }
        Sort newsorter = new Sort(newbase, numOfBuff, newattr);
        newsorter.setSchema((Schema) newbase.getSchema().clone());
        return newsorter;
    }

    /**
     * To check whether the current tuple is already present
     **/
    private boolean isDistinct(Tuple tuple1, Tuple tuple2) {
        int result;

        if (!this.attributeIndexes.isEmpty()) {
            for (Integer i : attributeIndexes) {
                result = Tuple.compareTuples(tuple1, tuple2, i);

                if (result != 0) {
                    return true;
                }
            }
            return false;

        } else {
            return !tuple1.equals(tuple2);
        }
    }

    // Debugging
    private void printSortedRun(SortedRun sr) {
        System.out.print("[");
        for (Tuple t : sr.getSortedTuples()) {
            System.out.print(t.dataAt(0));
            System.out.print(", ");
        }
        System.out.println("]");
    }

    private void printSortedRuns(ArrayList<SortedRun> sortedRuns) {
        int i = 1;
        for (SortedRun sr : sortedRuns) {
            System.out.print("SR #" + i + ": ");
            printSortedRun(sr);
            i++;
        }
    }

}