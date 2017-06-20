package org.wikipedia.citolytics.cpa.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;

/**
 * Filters with a recommendation set is complete, i.e. contains topK results.
 *
 * TODO Add validation test for output (article without any out- and in-links?)
 */
public class RecommendationSetFilter implements FilterFunction<RecommendationSet> {
    private int topK;
    private boolean complete;

    public RecommendationSetFilter(boolean complete, int topK) {
        this.complete = complete;
        this.topK = topK;
    }

    @Override
    public boolean filter(RecommendationSet set) throws Exception {
        if(complete) {
            return set.getResults().size() == topK;
        } else {
            return set.getResults().size() < topK;
        }

    }
}
