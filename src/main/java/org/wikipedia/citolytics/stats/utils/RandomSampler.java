package org.wikipedia.citolytics.stats.utils;

import org.apache.flink.api.common.functions.FilterFunction;

public class RandomSampler<T extends Object> implements FilterFunction<T> {
    private double p = 0.1;

    public RandomSampler(double p) {
        this.p = p;
    }

    @Override
    public boolean filter(T t) throws Exception {
        return Math.random() < p;
    }
}
