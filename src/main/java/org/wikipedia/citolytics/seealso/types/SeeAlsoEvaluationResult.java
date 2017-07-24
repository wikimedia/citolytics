package org.wikipedia.citolytics.seealso.types;

import org.apache.flink.api.java.tuple.Tuple11;

import java.util.Collection;

public class SeeAlsoEvaluationResult extends Tuple11<String, Collection<String>, Integer,
        WikiSimComparableResultList<Double>, Integer, Double, Double, Double, Integer, Integer, Integer> implements EvaluationResult {
    public SeeAlsoEvaluationResult() {
    }

    public final static int ARTICLE_KEY = 0;
    public final static int SEEALSO_LIST_KEY = 1;
    public final static int SEELASO_COUNT_KEY = 2;
    public final static int RECOMMENDATIONS_LIST_KEY = 3;
    public final static int RECOMMENDATIONS_COUNT_KEY = 4;
    public final static int MRR_KEY = 5;
    public final static int TOP_K_SCORE_KEY = 6;
    public final static int MAP_KEY = 7;
    public final static int RELEVANT_COUNT_1_KEY = 8;
    public final static int RELEVANT_COUNT_2_KEY = 9;
    public final static int RELEVANT_COUNT_3_KEY = 10;

    public SeeAlsoEvaluationResult(
            String article,
            Collection<String> seeAlsoLinks,
            int seeAlsoLinksCount,
            WikiSimComparableResultList<Double> recommendations,
            int recommendationsCount,
            double mrr,
            double topKScore,
            double map,
            int relevantCount1,
            int relevantCount2,
            int relevantCount3) {
        f0 = article;
        f1 = seeAlsoLinks;
        f2 = seeAlsoLinksCount;
        f3 = recommendations;
        f4 = recommendationsCount;
        f5 = mrr;
        f6 = topKScore;
        f7 = map;
        f8 = relevantCount1;
        f9 = relevantCount2;
        f10 = relevantCount3;
    }

    public String getArticle() {
        return f0;
    }

    @Override
    public String getTopRecommendations() {
        return null;
    }

    @Override
    public int getRecommendationsCount() {
        return f4;
    }

    public double getMRR() {
        return f5;
    }

    public double getTopKScore() {
        return f6;
    }

    public double getMAP() {
        return f7;
    }

    public int getRelevantCount1() {
        return f8;
    }

    @Override
    public int hashCode() {
        return f0.hashCode() + f1.hashCode() + f3.hashCode();
    }

    public static int[] getSummaryFields() {
        return new int[]{
                SEELASO_COUNT_KEY,
                RECOMMENDATIONS_COUNT_KEY,
                MRR_KEY,
                TOP_K_SCORE_KEY,
                MAP_KEY,
                RELEVANT_COUNT_1_KEY,
                RELEVANT_COUNT_2_KEY,
                RELEVANT_COUNT_3_KEY
        };
    }
}
