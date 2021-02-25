package qp.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

public class SortedRun implements Serializable {

    ArrayList<Tuple> sortedTuples;

    public SortedRun(ArrayList<Tuple> sortedTuples) {
        this.sortedTuples = sortedTuples;
    }

    public static int compareTuples(Tuple t1, Tuple t2, ArrayList<Integer> attributeIndexes) {
        int result = 0;
        for (int index: attributeIndexes) {
            result = Tuple.compareTuples(t1, t2, index);
            if (result != 0) {
                break;
            }
        }

        return result;
    }

    public SortedRun(ArrayList<Tuple> tuples, ArrayList<Integer> attributeIndexes, boolean isAsc) {

        if (isAsc) {
            tuples.sort((x, y) -> compareTuples(x, y, attributeIndexes));
        } else {
            tuples.sort((x, y) -> compareTuples(y, x, attributeIndexes));
        }

        this.sortedTuples = tuples;
    }

    public SortedRun(ArrayList<Tuple> tuples, ArrayList<Integer> attributeIndexes) {
        this(tuples, attributeIndexes, true);
    }

    public ArrayList<Tuple> getSortedTuples() {
        return this.sortedTuples;
    }

    public Tuple peek() {
        return sortedTuples.get(0);
    }

    public Tuple poll() {
        return sortedTuples.remove(0);
    }

    public boolean isEmpty() {
        return sortedTuples.isEmpty();
    }

    public int size() {
        return sortedTuples.size();
    }

}
