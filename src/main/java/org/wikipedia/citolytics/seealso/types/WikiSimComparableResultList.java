package org.wikipedia.citolytics.seealso.types;

import java.util.ArrayList;
import java.util.Collection;
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

}
