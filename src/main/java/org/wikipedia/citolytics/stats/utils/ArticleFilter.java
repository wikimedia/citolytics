package org.wikipedia.citolytics.stats.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;

import java.util.Collection;


public class ArticleFilter<T extends Tuple> implements FilterFunction<T> {
    private Collection<String> articles;
    private int field;

    public ArticleFilter(Collection<String> articles, int field) {
        this.articles = articles;
        this.field = field;
    }

    @Override
    public boolean filter(T recommendation) throws Exception {
        return articles.contains(recommendation.getField(field));
    }
}
