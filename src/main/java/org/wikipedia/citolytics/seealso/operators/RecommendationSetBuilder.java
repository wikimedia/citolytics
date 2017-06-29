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

public class RecommendationSetBuilder implements GroupReduceFunction<Recommendation, RecommendationSet> {
    private int maxQueueSize = 20;

    public RecommendationSetBuilder() {
    }

    public RecommendationSetBuilder(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    public void reduce(Iterable<Recommendation> in, Collector<RecommendationSet> out) throws Exception {
        Iterator<Recommendation> iterator = in.iterator();

        Recommendation recommendation = null;
        Map<String, WikiSimComparableResult> recommendations = new HashMap<>();

        // Default: MinQueue = Smallest elements a kept in queue
        // -> Change: MaxQueue = Keep greatest elements
        MinMaxPriorityQueue<WikiSimComparableResult<Double>> queue = MinMaxPriorityQueue
                .orderedBy(new Comparator<WikiSimComparableResult<Double>>() {
                    @Override
                    public int compare(WikiSimComparableResult<Double> o1, WikiSimComparableResult<Double> o2) {
                        return -1 * o1.compareTo(o2);
                    }
                })
                .maximumSize(maxQueueSize)
                .create();

        while (iterator.hasNext()) {
            recommendation = iterator.next();

            if(recommendations.containsKey(recommendation.getRecommendationTitle())) {
                throw new Exception("Duplicate recommendation: " + recommendation.getRecommendationTitle() + "; current list: " + recommendations);
            } else {
                recommendations.put(
                        recommendation.getRecommendationTitle(),
                        new WikiSimComparableResult<>(recommendation.getRecommendationTitle(), recommendation.getScore(), recommendation.getRecommendationId())
                        );
            }

            queue.add(new WikiSimComparableResult<>(recommendation.getRecommendationTitle(), recommendation.getScore(), recommendation.getRecommendationId()));
        }

        //  WikiSimComparableResultList<Double>
        out.collect(new RecommendationSet(recommendation.getSourceTitle(), recommendation.getSourceId(), new WikiSimComparableResultList<>(queue)));
    }
}
