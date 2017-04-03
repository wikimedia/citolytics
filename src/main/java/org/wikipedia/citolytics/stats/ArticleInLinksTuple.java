package org.wikipedia.citolytics.stats;

import org.apache.flink.api.java.tuple.Tuple2;

public class ArticleInLinksTuple extends Tuple2<String, Integer> {
    public ArticleInLinksTuple() {
        // Flink requires empty constructor
    }

    public ArticleInLinksTuple(String article, int linkCount) {
        f0 = article;
        f1 = linkCount;
    }
}
