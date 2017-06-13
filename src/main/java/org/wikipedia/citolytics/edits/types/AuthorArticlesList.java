package org.wikipedia.citolytics.edits.types;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class AuthorArticlesList extends Tuple2<Integer, List<String>> {
    public AuthorArticlesList() {
        // empty constructor
    }

    public AuthorArticlesList(int authorId, List<String> articles) {
        f0 = authorId;
        f1 = articles;
    }

    public List<String> getArticleList() {
        return f1;
    }
}
