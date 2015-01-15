package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by malteschwarzer on 15.01.15.
 */
public class ComparableResult<T extends Comparable> extends Tuple3<String, String, T> implements Comparable<ComparableResult<T>> {
    public final static int SORT1_FIELD = 2;
    public final static int SORT2_FIELD = 1;

    public ComparableResult(Tuple3<String, String, T> tuple) {
        setField(tuple.getField(0), 0);
        setField(tuple.getField(1), 1);
        setField(tuple.getField(1), 2);
    }

    public ComparableResult(String f0, String f1, T f2) {
        setField(f0, 0);
        setField(f1, 1);
        setField(f2, 2);
    }

    @Override
    public int compareTo(ComparableResult<T> other) {
        int firstSort = -1 * ((T) other.getField(SORT1_FIELD)).compareTo((T) getField(SORT1_FIELD));

        if (firstSort == 0) {
            // second sort
            return ((String) other.getField(SORT2_FIELD)).compareTo((String) getField(SORT2_FIELD));
        } else {
            return firstSort;
        }
    }
}
