package org.wikipedia.citolytics.edits.types;

import org.apache.flink.api.java.tuple.Tuple2;

public class ArticleAuthorPair extends Tuple2<String, Integer> {
    public ArticleAuthorPair() {
        // Flink requires empty constructor
    }

    public ArticleAuthorPair(String article, int authorId) {
        f0 = article;
        f1 = authorId;
    }

    public String getArticle() {
        return f0;
    }

    public int getAuthorId() {
        return f1;
    }
}
