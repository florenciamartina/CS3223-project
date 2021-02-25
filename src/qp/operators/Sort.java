/**
 * ExternalSort Operation
 **/

package qp.operators;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import qp.optimizer.BufferManager;
import qp.utils.*;

public class Sort extends Operator {

    int batchSize;  // Number of tuples per outbatch
    Operator base;
    ArrayList<Integer> attributeIndexes;
    ArrayList<Attribute> attributes;
    boolean isAsc;
    ObjectInputStream in;  // Input file being scanned

    /**
     * The following fields are required during
     * * execution of the Sort operator
     **/
    boolean eos;                // Indicate whether end of stream is reached or not
    Batch outBatch;             // This is the current output buffer

    // Sorting attributes
    int numOfBuff;
    int numSortedRuns;
    ArrayList<SortedRun> sortedRuns;
    int totalSize = 0;          // debugging purposes

    /**
     * constructor
     **/
    public Sort(Operator base, int numOfBuff, ArrayList<Attribute> attributeList, boolean isAsc) {
        super(OpType.EXTERNALSORT);
        this.base = base;
        this.schema = base.schema;
        this.numOfBuff = numOfBuff;
        this.attributes = attributeList;
        this.isAsc = isAsc;
    }

    public Sort(Operator base, int numOfBuff, ArrayList<Attribute> attributeList) {
        this(base, numOfBuff, attributeList, true);
    }

    public boolean open() {

        // Base is to be materialized for Sort to perform
        if (!base.open())
            return false;

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

        sortedRuns = mergeSortedRuns();

        return sortedRuns.size() == 1;
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

        while (!outBatch.isFull()) {
            if (sortedRuns.get(0).isEmpty()) {
                eos = true;
                return outBatch;
            }

            outBatch.add(sortedRuns.get(0).poll());
        }

        return outBatch;
}

    private boolean isSortedRunsEmpty (int end) {
        return sortedRuns.subList(0, end).stream().allMatch(SortedRun::isEmpty);
    }

    private int generateSortedRuns() {

        int numSortedRun = 0;
        Batch inputBatch;

        sortedRuns = new ArrayList<>();

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
            SortedRun sr = new SortedRun(tuples, attributeIndexes, isAsc);
            sortedRuns.add(sr);

            // Write sorted runs to temporary files
//            String filename = "sortedRunTemp-" + numSortedRun;
//            try {
//                ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(filename));
//                for (Tuple t: sr.getSortedTuples()) {
//                    outputStream.writeObject(t);
//                }
//
//                outputStream.close();
//            } catch (IOException io) {
//                System.out.println("Sort: Error writing to temporary file.");
//            }

            numSortedRun++;
        }

        return numSortedRun;
    }

    private ArrayList<SortedRun> mergeSortedRuns() {

        ArrayList<SortedRun> mergedSortedRuns;
        if (isAsc) {
            mergedSortedRuns = sortTupleAsc();
        } else {
            mergedSortedRuns = sortTupleDesc();
        }

        return mergedSortedRuns;
//
    }

    public ArrayList<SortedRun> sortTupleAsc() {

        while (sortedRuns.size() > 1) {

            Batch mergeOutput = new Batch(batchSize);
            int numOfMergedSortedRuns = Math.min(numOfBuff - 1, sortedRuns.size());

            SortedRun minSortedRun;
            while (!isSortedRunsEmpty(numOfMergedSortedRuns)) {
                minSortedRun = findMinimumSortedRun(numOfMergedSortedRuns);
                mergeOutput.add(minSortedRun.poll());
            }

            sortedRuns.removeIf(SortedRun::isEmpty);
            sortedRuns.add(new SortedRun(mergeOutput.getTuples()));
        }

        return sortedRuns;
    }

    public ArrayList<SortedRun> sortTupleDesc() {

        while (sortedRuns.size() > 1) {

            Batch mergeOutput = new Batch(batchSize);
            int numOfMergedSortedRuns = Math.min(numOfBuff - 1, sortedRuns.size());

            SortedRun maxSortedRun;
            while (!isSortedRunsEmpty(numOfMergedSortedRuns)) {
                maxSortedRun = findMaximumSortedRun(numOfMergedSortedRuns);
                mergeOutput.add(maxSortedRun.poll());
            }

            sortedRuns.removeIf(SortedRun::isEmpty);
            sortedRuns.add(new SortedRun(mergeOutput.getTuples()));
        }

        return sortedRuns;
    }

    private SortedRun findMinimumSortedRun(int numOfMergedSortedRuns) {
        Tuple minTuple = null;
        SortedRun minSortedRun = null;
        int compareResult;
        for (int i = 0; i < numOfMergedSortedRuns; i++) {

            if (sortedRuns.get(i).isEmpty()) {
                continue;
            }

            Tuple currTuple = sortedRuns.get(i).peek();


            if (minTuple == null) {
                minTuple = currTuple;
                minSortedRun = sortedRuns.get(i);
            }

            compareResult = SortedRun.compareTuples(currTuple, minTuple, attributeIndexes);

            if (compareResult >= 0) {
                continue;
            }

            minTuple = currTuple;
            minSortedRun = sortedRuns.get(i);
        }

        return minSortedRun;
    }

    private SortedRun findMaximumSortedRun(int numOfMergedSortedRuns) {
        Tuple maxTuple = null;
        SortedRun maxSortedRun = null;
        int compareResult;
        for (int i = 0; i < numOfMergedSortedRuns; i++) {

            if (sortedRuns.get(i).isEmpty()) {
                continue;
            }

            Tuple currTuple = sortedRuns.get(i).peek();


            if (maxTuple == null) {
                maxTuple = currTuple;
                maxSortedRun = sortedRuns.get(i);
            }

            compareResult = SortedRun.compareTuples(currTuple, maxTuple, attributeIndexes);

            if (compareResult >= 0) {
                continue;
            }

            maxTuple = currTuple;
            maxSortedRun = sortedRuns.get(i);
        }

        return maxSortedRun;
    }


    public Object clone() {
        Operator newbase = (Operator) base.clone();
        int numOfBuff = BufferManager.getBuffersPerJoin();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attributes.size(); ++i)
            newattr.add((Attribute) attributes.get(i).clone());
        Sort newsorter = new Sort(newbase, numOfBuff, newattr);
        newsorter.setSchema((Schema) newbase.getSchema().clone());
        return newsorter;
    }

    // Debugging
    private void printSortedRuns() {
        int i = 1;
        for (SortedRun sr: sortedRuns) {
            System.out.print("SR #" + i + ": " );
            System.out.print("[");
            for (Tuple t: sr.getSortedTuples()) {
                System.out.print(t.dataAt(0));
                System.out.print(", ");
            }
            System.out.println("]");
            i++;
        }
    }

}