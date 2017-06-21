package org.wikipedia.citolytics.stats;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiSimReader;
import org.wikipedia.citolytics.cpa.types.Recommendation;

/**
 * Extracts a sample from all recommendations for analyzing CPI values.
 */
public class CPISampler extends WikiSimAbstractJob<Tuple1<Double>> {
    @Override
    public void plan() throws Exception {
        String wikiSimInputFilename = getParams().getRequired("input");

        outputFilename = getParams().getRequired("output");
        int fieldScore = getParams().getInt("score", 5);
        int fieldPageA = getParams().getInt("page-a", 1);
        int fieldPageB = getParams().getInt("page-b", 2);
        double p = getParams().getDouble("p", 0.1);

        result = WikiSimReader.readWikiSimOutput(env, wikiSimInputFilename, fieldPageA, fieldPageB, fieldScore)
                .filter(new RandomSampler<Recommendation>(p))
                .map(new MapFunction<Recommendation, Tuple1<Double>>() {
                    @Override
                    public Tuple1<Double> map(Recommendation recommendation) throws Exception {
                        double decimals = 10000.0;
                        return new Tuple1<>(Math.round(recommendation.getScore() * decimals) / decimals);
                    }
                });


    }

    public class RandomSampler<T extends Tuple> implements FilterFunction<T> {
        private double p = 0.1;

        RandomSampler(double p) {
            this.p = p;
        }

        @Override
        public boolean filter(T t) throws Exception {
            return Math.random() < p;
        }
    }
}
