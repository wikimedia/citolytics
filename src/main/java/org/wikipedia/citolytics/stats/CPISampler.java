package org.wikipedia.citolytics.stats;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiSimReader;
import org.wikipedia.citolytics.cpa.types.Recommendation;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.stats.utils.RandomSampler;

/**
 * Extracts a sample from all recommendations for analyzing CPI values.
 */
public class CPISampler extends WikiSimAbstractJob<Tuple1<Double>> {

    public static void main(String[] args) throws Exception {
        new CPISampler().start(args);
    }

    @Override
    public void plan() throws Exception {
        String wikiSimInputFilename = getParams().getRequired("input");

        outputFilename = getParams().getRequired("output");
        int fieldScore = getParams().getInt("score", RecommendationPair.CPI_LIST_KEY);
        int fieldPageA = getParams().getInt("page-a", RecommendationPair.PAGE_A_KEY);
        int fieldPageB = getParams().getInt("page-b", RecommendationPair.PAGE_B_KEY);
        int fieldPageIdA = getParams().getInt("page-id-a", RecommendationPair.PAGE_A_ID_KEY);
        int fieldPageIdB = getParams().getInt("page-id-b", RecommendationPair.PAGE_B_ID_KEY);

        double p = getParams().getDouble("p", 0.1);

        result = WikiSimReader.readWikiSimOutput(env, wikiSimInputFilename, fieldPageA, fieldPageB, fieldScore, fieldPageIdA, fieldPageIdB)
                .filter(new RandomSampler<Recommendation>(p))
                .map(new MapFunction<Recommendation, Tuple1<Double>>() {
                    @Override
                    public Tuple1<Double> map(Recommendation recommendation) throws Exception {
                        return new Tuple1<>(roundToDecimals(recommendation.getScore(), 5));
                    }
                });
    }

    public static double roundToDecimals(double value, int decimals) {
        double f = Math.pow(10, decimals);
        return Math.round(value * f) / f;
    }


}
