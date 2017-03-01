package org.wikipedia.citolytics.cpa.types;

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Represents a single recommendation from A -> B.
 */
public class WikiSimSingleResult extends Tuple5<String, String, Double, Integer, Integer> {
    public WikiSimSingleResult() {}

    @Deprecated
    public WikiSimSingleResult(String sourceTitle, String recommendationTitle, Double score) {
        f0 = sourceTitle;
        f1 = recommendationTitle;
        f2 = score;
        f3 = 0;
        f4 = 0;
    }

    public WikiSimSingleResult(String sourceTitle, String recommendationTitle, Double score, int sourceId, int recommendationId) {
        f0 = sourceTitle;
        f1 = recommendationTitle;
        f2 = score;
        f3 = sourceId;
        f4 = recommendationId;
    }

    public String getSourceTitle() {
        return f0;
    }

    public String getRecommendationTitle() {
        return f1;
    }

    public Double getScore() {
        return f2;
    }

    public int getSourceId() {
        return f3;
    }

    public int getRecommendationId() {
        return f4;
    }
}
