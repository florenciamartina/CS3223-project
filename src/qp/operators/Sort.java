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
    int numOfBuff;  // Number of block
    ArrayList<Integer> attributeIndexes;
    ArrayList<Attribute> attributes;
    String tabname;
    String filename;       // Corresponding file name
    boolean isAsc;
    ObjectInputStream in;  // Input file being scanned
    int totalSize = 0;


    /**
     * The following fields are required during
     * * execution of the Sort operator
     **/
    boolean eos;                // Indicate whether end of stream is reached or not
    ArrayList<Batch> inBatch;   // This is the current input buffer
    Batch outBatch;             // This is the current output buffer

    // Sorting attributes
    int numSortedRuns;
    ArrayList<SortedRun> sortedRuns;

    /**
     * constructor
     **/
    public Sort(int type, Operator base, int numOfBuff, ArrayList<Attribute> attributeList, String tabname) {
        super(type);
        this.base = base;
        this.schema = base.schema;
        this.numOfBuff = numOfBuff;
        this.attributes = attributeList;
        this.tabname = tabname;
        this.isAsc = true;
        filename = tabname + ".tbl";
    }

    public Sort(int type, Operator base, int numOfBuff, ArrayList<Attribute> attributeList, String tabname, boolean isAsc) {
        super(type);
        this.base = base;
        this.schema = base.schema;
        this.numOfBuff = numOfBuff;
        this.attributes = attributeList;
        this.tabname = tabname;
        this.isAsc = isAsc;
        filename = tabname + ".tbl";
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

        // Read the table
        try {
            in = new ObjectInputStream(new FileInputStream(filename));
        } catch (Exception e) {
            System.err.println(" Error reading " + filename);
            return false;
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

        inBatch = new ArrayList<>();

        /** An output buffer is initiated **/
        outBatch = new Batch(batchSize);

        while (!outBatch.isFull()) {
            for (Tuple t: sortedRuns.get(0).getSortedTuples()) {
                outBatch.add(t);
                totalSize++;
            }

            eos = true;
            return outBatch;

//            try {
//                Tuple data = (Tuple) in.readObject();
//                outBatch.add(data);
//                totalSize++;
//            } catch (ClassNotFoundException cnf) {
//                System.err.println("Sort:Class not found for reading file  " + cnf);
//                System.exit(1);
//            } catch (EOFException EOF) {
//                /** At this point incomplete page is sent and at next call it considered
//                 ** as end of file
//                 **/
//                eos = true;
//                return outBatch;
//            } catch (IOException e) {
//                System.err.println("Sort:Error reading " + e);
//                System.exit(1);
//            }
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
//        int pass = 1;
//        while (sortedRuns.size() > 1) {
//
//            Batch mergeOutput = new Batch(batchSize);
//            int numOfMergedSortedRuns = Math.min(numOfBuff - 1, sortedRuns.size());
//
//            while (!isSortedRunsEmpty(numOfMergedSortedRuns)) {
//
//                // Find smallest value among (numOfBuff - 1) sorted runs
//                Tuple minTuple = null;
//                SortedRun minSortedRun = null;
//                int compareResult;
//                for (int i = 0; i < numOfMergedSortedRuns; i++) {
//
//                    if (sortedRuns.get(i).isEmpty()) {
//                        continue;
//                    }
//
//                    Tuple currTuple = sortedRuns.get(i).peek();
//
//                    if (minTuple == null) {
//                        minTuple = currTuple;
//                        minSortedRun = sortedRuns.get(i);
//                    }
//
//                    compareResult = SortedRun.compareTuples(currTuple, minTuple, attributeIndexes);
//
//
//
//                    if (compareResult >= 0) {
//                        continue;
//                    }
//
//
//
//                    minTuple = currTuple;
//                    minSortedRun = sortedRuns.get(i);
//                }
//
//                mergeOutput.add(minTuple);
//                minSortedRun.poll();
//            }
//
//            sortedRuns.removeIf(SortedRun::isEmpty);
//            sortedRuns.add(new SortedRun(mergeOutput.getTuples()));
//        }
//
//        return sortedRuns;
    }

    public ArrayList<SortedRun> sortTupleAsc() {

        int pass = 1;
        while (sortedRuns.size() > 1) {

            Batch mergeOutput = new Batch(batchSize);
            int numOfMergedSortedRuns = Math.min(numOfBuff - 1, sortedRuns.size());

            while (!isSortedRunsEmpty(numOfMergedSortedRuns)) {

                // Find smallest value among (numOfBuff - 1) sorted runs
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

                mergeOutput.add(minTuple);
                minSortedRun.poll();
            }

            sortedRuns.removeIf(SortedRun::isEmpty);
            sortedRuns.add(new SortedRun(mergeOutput.getTuples()));
        }

        return sortedRuns;
    }

    public ArrayList<SortedRun> sortTupleDesc() {

        int pass = 1;
        while (sortedRuns.size() > 1) {

            Batch mergeOutput = new Batch(batchSize);
            int numOfMergedSortedRuns = Math.min(numOfBuff - 1, sortedRuns.size());

            while (!isSortedRunsEmpty(numOfMergedSortedRuns)) {

                // Find biggest value among (numOfBuff - 1) sorted runs
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

                    if (compareResult <= 0) {
                        continue;
                    }

                    maxTuple = currTuple;
                    maxSortedRun = sortedRuns.get(i);
                }

                mergeOutput.add(maxTuple);
                maxSortedRun.poll();
            }

            sortedRuns.removeIf(SortedRun::isEmpty);
            sortedRuns.add(new SortedRun(mergeOutput.getTuples()));
        }

        return sortedRuns;
    }


    public Object clone() {
        Operator newbase = (Operator) base.clone();
        int numOfBuff = BufferManager.getBuffersPerJoin();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attributes.size(); ++i)
            newattr.add((Attribute) attributes.get(i).clone());
        Sort newsorter = new Sort(optype, newbase, numOfBuff, newattr, tabname);
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