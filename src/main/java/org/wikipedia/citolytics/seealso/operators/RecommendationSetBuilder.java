package org.wikipedia.citolytics.seealso.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.shaded.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.Recommendation;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Build from WikiSim recommendations the final top-k result sets (unsorted).
 *
 * Additionally, it checks for duplicates in results (may decrease performance).
 */
public class RecommendationSetBuilder implements GroupReduceFunction<Recommendation, RecommendationSet> {
    private int topK = 20;
    private final static boolean ignoreIdMismatch = true;

    public RecommendationSetBuilder(int topK) {
        this.topK = topK;
    }

    @Override
    public void reduce(Iterable<Recommendation> in, Collector<RecommendationSet> out) throws Exception {
        Iterator<Recommendation> iterator = in.iterator();

        Recommendation recommendation = null;

        // Handle duplicate recommendations first (TODO How can this happen? Redirect resolver?)
        Map<String, WikiSimComparableResult> recommendations = new HashMap<>();
        while (iterator.hasNext()) {
            recommendation = iterator.next();

            if (recommendations.containsKey(recommendation.getRecommendationTitle())) {
                int aId = recommendations.get(recommendation.getRecommendationTitle()).getId();
                int bId = recommendation.getRecommendationId();
                //throw new Exception("Duplicate recommendation: " + recommendation.getRecommendationTitle() + "; current list: " + recommendations);
//                if (!ignoreIdMismatch && aId != bId) {
//                    throw new Exception("Invalid IDs for duplicate recommendations (" + aId + " != " + bId +"): A=" + recommendation + "; B = " + recommendations.get(recommendation.getRecommendationTitle()));
//                }

                // Sum of both scores
                Double score = recommendation.getScore() + (Double) recommendations.get(recommendation.getRecommendationTitle()).getSortField1();

                recommendations.put(recommendation.getRecommendationTitle(),
                        new WikiSimComparableResult<>(
                                recommendation.getRecommendationTitle(),
                                score,
                                recommendation.getRecommendationId())
                );
            } else {
                recommendations.put(
                        recommendation.getRecommendationTitle(),
                        new WikiSimComparableResult<>(recommendation.getRecommendationTitle(), recommendation.getScore(), recommendation.getRecommendationId())
                );
            }
        }

        // Default: MinQueue = Smallest elements a kept in queue
        // -> Change: MaxQueue = Keep greatest elements
        MinMaxPriorityQueue<WikiSimComparableResult<Double>> queue = MinMaxPriorityQueue
                .orderedBy(new Comparator<WikiSimComparableResult<Double>>() {
                    @Override
                    public int compare(WikiSimComparableResult<Double> o1, WikiSimComparableResult<Double> o2) {
                        return -1 * o1.compareTo(o2);
                    }
                })
                .maximumSize(topK)
                .create();

        for(WikiSimComparableResult<Double> result: recommendations.values()) {
            queue.add(result);
        }

        //  WikiSimComparableResultList<Double>
        out.collect(new RecommendationSet(recommendation.getSourceTitle(), recommendation.getSourceId(), new WikiSimComparableResultList<>(queue)));
    }
}
