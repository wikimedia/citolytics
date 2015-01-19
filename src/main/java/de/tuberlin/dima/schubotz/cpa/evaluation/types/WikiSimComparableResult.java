package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Makes WikiSimResults comparable
 * - Sort1 Field: Ordered Descending (CoCit/CPA scores)
 * - Sort2 Field: Ordered Ascending (Article recommendation)
 */
public class WikiSimComparableResult<T extends Comparable> extends Tuple3<String, String, T> implements Comparable<WikiSimComparableResult<T>> {
    public final static int SORT1_FIELD = 2;
    public final static int SORT2_FIELD = 1;

    public WikiSimComparableResult(Tuple3<String, String, T> tuple) {
        setField(tuple.getField(0), 0);
        setField(tuple.getField(1), 1);
        setField(tuple.getField(1), 2);
    }

    public WikiSimComparableResult(String f0, String f1, T f2) {
        setField(f0, 0);
        setField(f1, 1);
        setField(f2, 2);
    }

    @Override
    public int compareTo(WikiSimComparableResult<T> other) {
        // first sort (descending)
        int firstSort = -1 * ((T) other.getField(SORT1_FIELD)).compareTo((T) getField(SORT1_FIELD));

        if (firstSort == 0) {
            // second sort (ascending)
            return ((String) other.getField(SORT2_FIELD)).compareTo((String) getField(SORT2_FIELD));
        } else {
            return firstSort;
        }
    }
}
