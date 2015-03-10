package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class WikiSimComparableResultList<SORT extends Comparable> extends ArrayList<WikiSimComparableResult<SORT>> {
    public WikiSimComparableResultList(List<WikiSimComparableResult<SORT>> list) {
        super(list);
    }

    public WikiSimComparableResultList(Collection<WikiSimComparableResult<SORT>> list) {
        super(list);
    }

    public WikiSimComparableResultList() {
        super();
    }

    public static List<String> getNamesAsList(List<WikiSimComparableResult> resultList) {
        List<String> names = new ArrayList<>();
        Iterator<WikiSimComparableResult> iterator = resultList.listIterator();

        while (iterator.hasNext()) {
            WikiSimComparableResult result = iterator.next();

            names.add(result.getName());
        }
        return names;
    }

    public static List<String> getNamesAsList(WikiSimComparableResultList resultList) {
        List<String> names = new ArrayList<>();
        Iterator<WikiSimComparableResult> iterator = resultList.listIterator();

        while (iterator.hasNext()) {
            WikiSimComparableResult result = iterator.next();

            names.add(result.getName());
        }
        return names;
    }

    public static String[] getNamesAsArray(WikiSimComparableResultList resultList) {
        String[] names = new String[resultList.size()];
        Iterator<WikiSimComparableResult> iterator = resultList.listIterator();
        int i = 0;
        while (iterator.hasNext()) {
            WikiSimComparableResult result = iterator.next();
            names[i] = result.getName();
            i++;
        }
        return names;
    }
}
