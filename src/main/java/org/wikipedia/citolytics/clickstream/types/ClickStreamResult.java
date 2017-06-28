package org.wikipedia.citolytics.clickstream.types;

import org.apache.flink.api.java.tuple.Tuple9;

import java.util.ArrayList;

/**
 * article | retrieved documents | number of ret. documents | impressions | sum out clicks | k=10 clicks | k=5 clicks | k=1 clicks | optimal clicks
 * <p/>
 * OLD:
 * article | retrieved documents | number of ret. documents | counter | k=10 rel clicks | k=10 clicks | k=5 rel clicks | k=5 clicks | k=1 rel clicks | k=1 clicks
 */
public class ClickStreamResult extends
        //    Tuple10<String, ArrayList<Tuple4<String, Double, Integer, Double>>, Integer, Integer, Integer, Double, Integer, Double, Integer, Double>
        Tuple9<String, ArrayList<ClickStreamRecommendationResult>, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
    public final static int ARTICLE_KEY = 0;
    public final static int RECOMMENDATIONS_LIST_KEY = 1;
    public final static int RECOMMENDATIONS_COUNT_KEY = 2;
    public final static int IMPRESSIONS_KEY = 3;
    public final static int CLICKS_KEY = 4;
    public final static int CLICKS_K1_KEY = 5;
    public final static int CLICKS_K2_KEY = 6;
    public final static int CLICKS_K3_KEY = 7;
    public final static int OPTIMAL_CLICKS = 8;

    public ClickStreamResult() {
    }

    public ClickStreamResult(String article, ArrayList<ClickStreamRecommendationResult> recommendations, int recommendationsCount,
                             int impressions, int totalClicks, int clicks1, int clicks2, int clicks3, int optimalClicks) {
        f0 = article;
        f1 = recommendations;
        f2 = recommendationsCount;
        f3 = impressions;
        f4 = totalClicks;
        f5 = clicks1;
        f6 = clicks2;
        f7 = clicks3;
        f8 = optimalClicks;
    }

    public String getArticle() {
        return getField(ARTICLE_KEY);
    }

    public ArrayList<ClickStreamRecommendationResult> getRecommendations() {
        return getField(RECOMMENDATIONS_LIST_KEY);
    }

    public int getRecommendationsCount() {
        return getField(RECOMMENDATIONS_COUNT_KEY);
    }

    public int getImpressions() {
        return getField(IMPRESSIONS_KEY);
    }

    public int getTotalClicks() {
        return getField(CLICKS_KEY);
    }

    public int getClicks1() {
        return getField(CLICKS_K1_KEY);
    }

    public int getClicks2() {
        return getField(CLICKS_K2_KEY);
    }

    public int getClicks3() {
        return getField(CLICKS_K3_KEY);
    }

    public int getOptimalClicks() {
        return getField(OPTIMAL_CLICKS);
    }
}
