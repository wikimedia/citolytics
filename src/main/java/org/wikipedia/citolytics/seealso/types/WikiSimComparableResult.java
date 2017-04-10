package org.wikipedia.citolytics.seealso.types;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Makes WikiSimResults comparable
 * - Sort1 Field: Ordered Descending (CoCit/CPA scores)
 * - Sort2 Field: Ordered Ascending (Article recommendation)
 *
 * - Field 3: id
 */
public class WikiSimComparableResult<T extends Comparable> extends Tuple3<String, T, Integer> implements Comparable<WikiSimComparableResult<T>> {
    public final static int SORT1_FIELD = 1;
    public final static int SORT2_FIELD = 0;

    public T getSortField1() {
        return getField(SORT1_FIELD);
    }

    public String getSortField2() {
        return getField(SORT2_FIELD);
    }

    public String getName() {
        return f0;
    }

    public int getId() {
        return f2;
    }


    public WikiSimComparableResult(String title, T score, Integer id) {
        f0 = title;
        f1 = score;
        f2 = id;
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
