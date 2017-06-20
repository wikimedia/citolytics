package org.wikipedia.citolytics.stats;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Name
 * Words
 * Headlines
 * OutLinks
 * AvgLinkDistance
 * OutLinksPerWords
 */
public class ArticleStatsTuple extends Tuple6<String, Integer, Integer, Integer, Double, Integer> {
    public final static int ARTICLE_NAME_KEY = 0;
    public final static int WORDS_KEY = 1;
    public final static int HEADLINES_KEY = 2;
    public final static int OUT_LINKS_KEY = 3;
    public final static int AVG_LINK_DISTANCE_KEY = 4;
    public final static int IN_LINKS_KEY = 5;

    public ArticleStatsTuple() {
        // Flink requires empty constructor
    }

    public ArticleStatsTuple(String name, int words, int headlines, int outLinks, double avgLinkDistance) {
        setField(name, ARTICLE_NAME_KEY);
        setField(words, WORDS_KEY);
        setField(headlines, HEADLINES_KEY);
        setField(outLinks, OUT_LINKS_KEY);
        setField(avgLinkDistance, AVG_LINK_DISTANCE_KEY);
        setField(0, IN_LINKS_KEY);
    }

    public ArticleStatsTuple(String name, int words, int headlines, int outLinks, double avgLinkDistance, int inLinks) {
        setField(name, ARTICLE_NAME_KEY);
        setField(words, WORDS_KEY);
        setField(headlines, HEADLINES_KEY);
        setField(outLinks, OUT_LINKS_KEY);
        setField(avgLinkDistance, AVG_LINK_DISTANCE_KEY);
        setField(inLinks, IN_LINKS_KEY);
    }

    public String getArticle() {
        return getField(ARTICLE_NAME_KEY);
    }

    public int getWords() {
        return getField(WORDS_KEY);
    }

    public int getHeadlines() {
        return getField(HEADLINES_KEY);
    }

    public int getOutLinks() {
        return getField(OUT_LINKS_KEY);
    }

    public int getInLinks() {
        return getField(IN_LINKS_KEY);
    }

    public void setInLinks(int count) {
        setField(count, IN_LINKS_KEY);
    }

}
