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
        for (int i = 0; i < attributeIndexes.size(); i++) {
            result = Tuple.compareTuples(t1, t2, attributeIndexes.get(i));
            if (result != 0) break;
        }

        return result;
    }

    public SortedRun(ArrayList<Tuple> tuples, ArrayList<Integer> attributeIndexes) {
        //TODO: sort buffers
        if (tuples == null) {
            this.sortedTuples = new ArrayList<>();
        } else {
            tuples.sort((x, y) -> compareTuples(x, y, attributeIndexes));
            this.sortedTuples = tuples;
        }

    }

    public ArrayList<Tuple> getSortedTuples() {
        return this.sortedTuples;
    }

    public Tuple removeFromTuples(int index) {
        return this.sortedTuples.remove(index);
    }

}
