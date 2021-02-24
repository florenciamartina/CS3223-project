/**
 * ExternalSort Operation
 **/

package qp.operators;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import qp.optimizer.BufferManager;
import qp.utils.*;

public class Sort extends Operator {

    int batchsize;  // Number of tuples per outbatch
    ArrayList<SortedRun> sortedRuns;
    Operator base;
    int numOfBuff;  // Number of block
    ArrayList<Integer> attributeIndexes;
    ArrayList<Attribute> attributes;
    String tabname;
    String filename;       // Corresponding file name
    ObjectInputStream in;  // Input file being scanned


    /**
     * The following fields are required during
     * * execution of the distinct operator
     **/
    boolean eos;     // Indicate whether end of stream is reached or not
    ArrayList<Tuple> buffers;
    ArrayList<Batch> inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer

    /**
     * constructor
     **/
    public Sort(int type, Operator base, int numOfBuff, ArrayList<Attribute> attributeList, String tabname) {
        super(type);
        this.base = base;
        this.numOfBuff = numOfBuff;
        this.sortedRuns = new ArrayList<>();
        this.attributes = attributeList;
        this.tabname = tabname;
        filename = tabname + ".tbl";
    }

    public Batch next() {
        if (eos) {
            close();
            return null;
        }

        buffers = new ArrayList<>();
        inbatch = new ArrayList<>();            // Inbatch after sorted runs

        /** An output buffer is initiated **/
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
            try {
                Tuple data = (Tuple) in.readObject();
//                ArrayList<Tuple> tuples = sortedRuns.get(0).getSortedTuples();
//                for (Tuple tuple : tuples) {
//                    outbatch.add(tuple);
//                }
                outbatch.add(data);

            } catch (ClassNotFoundException cnf) {
                System.err.println("Sort:Class not found for reading file  " + cnf);
                System.exit(1);
            } catch (EOFException EOF) {
                /** At this point incomplete page is sent and at next call it considered
                 ** as end of file
                 **/
                eos = true;
                return outbatch;
            } catch (IOException e) {
                System.err.println("Sort:Error reading " + e);
                System.exit(1);
            }
        }

        return outbatch;
}

    public boolean open() {

        if (!base.open())
            return false;

        eos = false;  // Since the stream is just opened
        start = 0;    // Set the cursor to starting position in input buffer

        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize; // Number of tuples

        try {
            in = new ObjectInputStream(new FileInputStream(filename));
        } catch (Exception e) {
            System.err.println(" Error reading " + filename);
            return false;
        }

        this.attributeIndexes = new ArrayList<>();
        for (Attribute a : attributes) {
            attributeIndexes.add(schema.indexOf(a));
        }

        generateSortedRuns();

        // Merge sorted runs
        sortedRuns = mergeSortedRuns(sortedRuns);
        return sortedRuns.size() == 1;

    }

    private boolean isFinishedMerging(ArrayList<SortedRun> sortedRuns, int start, int end) {
        return sortedRuns.subList(start, end).stream().allMatch(x -> x.getSortedTuples().isEmpty());
    }

    private void generateSortedRuns() {
        Batch batch;
        buffers = new ArrayList<>();
        System.out.println(numOfBuff);
        int k = 0;
        while ((batch = base.next()) != null) {

            for (int i = 0; i < numOfBuff; i++) {
                for (int j = 0; j < batch.size(); j++) {
                    buffers.add(batch.get(j));
                }

                batch = base.next();

                if (batch == null) {
                    break;
                }
            }
//            System.out.print("hoho ");
//            System.out.println(buffers.size());

            SortedRun sr = new SortedRun(buffers, attributeIndexes);
            sortedRuns.add(sr);

            String filename = "sortedRun-"+k;
            try {
                ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(filename));
                for (Tuple t: sr.getSortedTuples()) {
                    outputStream.writeObject(t);
                }

                outputStream.close();
            } catch (IOException io) {
                System.out.println("io error");
            }

            k++;
        }
    }

    private ArrayList<SortedRun> mergeSortedRuns(ArrayList<SortedRun> sortedRuns) {

        SortedRun curr = sortedRuns.get(0);
        SortedRun smallest = curr;
        ArrayList<Tuple> tuples = curr.getSortedTuples();
        Tuple smallestTuple = tuples.size() > 0 ? tuples.get(0) : new Tuple(new ArrayList<>(Collections.singletonList(Integer.MAX_VALUE)));
        ArrayList<Tuple> sortedTuples = new ArrayList<>();
        while (sortedRuns.size() > 1) {

            // Check whether all tuples are merged
            while (!isFinishedMerging(sortedRuns, 0, Math.min(numOfBuff - 1, sortedRuns.size()))) {

                // Iterate through numOfBuff - 1 sorted runs at a time
                for (int i = 0; i < numOfBuff - 1 && i < sortedRuns.size(); i++) {

//                    if (sortedRuns.get(i).getSortedTuples().isEmpty()) {
//                        continue;
//                    }

                    try {
                        in = new ObjectInputStream(new FileInputStream(filename));
                        eos = false;
                    } catch (IOException io) {
                        System.err.println("NestedJoin:error in reading the file");
                        System.exit(1);
                    }

                    curr = sortedRuns.get(i);
                    tuples = curr.getSortedTuples();
                    Tuple temp = tuples.get(0);

                    // Compare with current smallest tuple
                    if (SortedRun.compareTuples(temp, smallestTuple, attributeIndexes) < 0) {
                        smallest = sortedRuns.get(i);
                        smallestTuple = temp;
                        System.out.println("SORTINGGGGGGG");
                    }
                }

                sortedTuples.add(smallest.removeFromTuples(0));

                // Remove merged sorted runs
                for (int j = 0; j < numOfBuff - 1 && !sortedRuns.isEmpty(); j++) {
                    sortedRuns.remove(0);
                }
            }

            sortedRuns.add(new SortedRun(sortedTuples));
        }

        return sortedRuns;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        int numOfBuff = BufferManager.getBuffersPerJoin();
        System.out.println(numOfBuff);
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attributes.size(); ++i)
            newattr.add((Attribute) attributes.get(i).clone());
        Sort newsorter = new Sort(optype, newbase, numOfBuff, newattr, tabname);
        newsorter.setSchema((Schema) newbase.getSchema().clone());
        return newsorter;
    }

}