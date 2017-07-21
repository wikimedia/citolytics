package org.wikipedia.citolytics.seealso.types;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Set;

/**
 * Represents the "See also" links of an article (title of source article). "See also" links must be unique.
 */
public class SeeAlsoLinks extends Tuple2<String, Set<String>> {
    public SeeAlsoLinks() {
        // Flink requires empty constructor
    }

    public SeeAlsoLinks(String article, Set<String> links) {
        f0 = article;
        f1 = links;
    }

    public String getArticle() {
        return f0;
    }

    public Set<String> getLinks() {
        return f1;
    }

    public void merge(SeeAlsoLinks other) {
        getLinks().addAll(other.getLinks());
    }
}

