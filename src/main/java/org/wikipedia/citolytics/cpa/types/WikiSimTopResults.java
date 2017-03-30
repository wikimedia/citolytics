package org.wikipedia.citolytics.cpa.types;

import org.apache.flink.api.java.tuple.Tuple3;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

/**
 * Represents the top-k recommendations with corresponding scores for a single page.
 */
public class WikiSimTopResults extends Tuple3<String, WikiSimComparableResultList<Double>, Integer> {
    public final static int SOURCE_TITLE_KEY = 0;

    public WikiSimTopResults() {

    }

    @Deprecated
    public WikiSimTopResults(String sourceTitle, WikiSimComparableResultList<Double> resultsList) {
        f0 = sourceTitle;
        f1 = resultsList;
        f2 = 0;
    }

    public WikiSimTopResults(String sourceTitle, int sourceId, WikiSimComparableResultList<Double> resultsList) {
        f0 = sourceTitle;
        f1 = resultsList;
        f2 = sourceId;
    }

    public WikiSimComparableResultList<Double> getResults() {
        return f1;
    }

    public String getSourceTitle() {
        return getField(SOURCE_TITLE_KEY);
    }

    public int getSourceId() {
        return f2;
    }
}
