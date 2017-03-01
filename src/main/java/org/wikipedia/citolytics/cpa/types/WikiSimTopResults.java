package org.wikipedia.citolytics.cpa.types;

import org.apache.flink.api.java.tuple.Tuple3;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

/**
 * Represents the top-k recommendations with corresponding scores for a single page.
 */
public class WikiSimTopResults extends Tuple3<String, WikiSimComparableResultList<Double>, Integer> {
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
        return f0;
    }

    public int getSourceId() {
        return f2;
    }
}
