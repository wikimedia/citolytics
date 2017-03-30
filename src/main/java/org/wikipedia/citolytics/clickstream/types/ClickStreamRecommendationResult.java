package org.wikipedia.citolytics.clickstream.types;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Click stream result for a single recommendation
 *
 * - recommended article name
 * - recommendation score
 * - clicks
 *
 */
public class ClickStreamRecommendationResult extends Tuple3<String, Double, Integer> {
    public ClickStreamRecommendationResult() {
        // Flink requires empty constructor
    }

    public ClickStreamRecommendationResult(String recommendedArticle, double recommendationScore, int clicks) {
        f0 = recommendedArticle;
        f1 = recommendationScore;
        f2 = clicks;
    }

    public String getRecommendedArticle() {
        return f0;
    }

    public double getRecommendationScore() {
        return f1;
    }

    public int getClicks() {
        return f2;
    }
}
