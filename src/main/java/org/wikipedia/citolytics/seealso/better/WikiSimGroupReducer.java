package org.wikipedia.citolytics.seealso.better;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.shaded.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.WikiSimRecommendation;
import org.wikipedia.citolytics.cpa.types.WikiSimRecommendationSet;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

import java.util.Comparator;
import java.util.Iterator;

public class WikiSimGroupReducer implements GroupReduceFunction<WikiSimRecommendation, WikiSimRecommendationSet> {
    private int maxQueueSize = 20;

    public WikiSimGroupReducer() {
    }

    public WikiSimGroupReducer(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    public void reduce(Iterable<WikiSimRecommendation> in, Collector<WikiSimRecommendationSet> out) throws Exception {
        Iterator<WikiSimRecommendation> iterator = in.iterator();

        WikiSimRecommendation joinRecord = null;

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
            joinRecord = iterator.next();
            queue.add(new WikiSimComparableResult<>(joinRecord.getRecommendationTitle(), joinRecord.getScore(), joinRecord.getRecommendationId()));
        }

        //  WikiSimComparableResultList<Double>
        out.collect(new WikiSimRecommendationSet(joinRecord.getSourceTitle(), joinRecord.getSourceId(), new WikiSimComparableResultList<>(queue)));
    }
}
