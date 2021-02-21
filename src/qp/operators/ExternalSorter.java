/**
 * ExternalSort Operation
 **/

package qp.operators;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import qp.optimizer.BufferManager;
import qp.utils.*;

public class ExternalSorter extends Operator {

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
    public ExternalSorter(int type, Operator base, int numOfBuff, ArrayList<Attribute> attributeList, String tabname) {
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
                outbatch.add(data);
            } catch (ClassNotFoundException cnf) {
                System.err.println("Scan:Class not found for reading file  " + cnf);
                System.exit(1);
            } catch (EOFException EOF) {
                /** At this point incomplete page is sent and at next call it considered
                 ** as end of file
                 **/
                eos = true;
                return outbatch;
            } catch (IOException e) {
                System.err.println("Scan:Error reading " + e);
                System.exit(1);
            }
        }

        return outbatch;
}


    public boolean open() {
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

        System.out.println(schema);
        System.out.println(base);
        for (Attribute a : attributes) {
            attributeIndexes.add(schema.indexOf(a));
        }


        if (!base.open())
            return false;

        generateSortedRuns();

        // Merge sorted runs
        return mergeSortedRuns(sortedRuns).size() == 1;
    }

    private boolean isFinishedMerging(ArrayList<SortedRun> sortedRuns, int start, int end) {
        return sortedRuns.subList(start, end).stream().allMatch(x -> x.getSortedTuples().isEmpty());
    }

    private void generateSortedRuns() {
        Batch batch;
        buffers = new ArrayList<>();
        System.out.println(numOfBuff);
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
            System.out.print("hoho ");
            System.out.println(buffers.size());

            sortedRuns.add(new SortedRun(buffers, attributeIndexes));
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

                    if (sortedRuns.get(i).getSortedTuples().isEmpty()) {
                        continue;
                    }

                    curr = sortedRuns.get(i);
                    tuples = curr.getSortedTuples();
                    Tuple temp = tuples.get(0);

                    // Compare with current smallest tuple
                    if (SortedRun.compareTuples(temp, smallestTuple, attributeIndexes) < 0) {
                        smallest = sortedRuns.get(i);
                        smallestTuple = temp;
                    }
                }

                sortedTuples.add(smallest.removeFromTuples(0));

                // Remove merged sorted runs
                for (int j = 0; j < numOfBuff - 1; j++) {
                    sortedRuns.remove(0);
                }
            }

            sortedRuns.add(new SortedRun(sortedTuples));
        }

        return sortedRuns;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        int numOfBuff = BufferManager.getNumberOfBuffer();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attributes.size(); ++i)
            newattr.add((Attribute) attributes.get(i).clone());
        ExternalSorter newsorter = new ExternalSorter(optype, newbase, numOfBuff, newattr, tabname);
        newsorter.setSchema((Schema) newbase.getSchema().clone());
        return newsorter;
    }

}