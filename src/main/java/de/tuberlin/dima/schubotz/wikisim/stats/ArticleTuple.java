package de.tuberlin.dima.schubotz.wikisim.stats;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Name
 * Words
 * Headlines
 * OutLinks
 * AvgLinkDistance
 * OutLinksPerWords
 */
public class ArticleTuple extends Tuple6<String, Integer, Integer, Integer, Double, Double> {
    public ArticleTuple(String name, int words, int headlines, int outLinks, double avgLinkDistance, double outLinksPerWords) {
        setField(String.valueOf(name), 0);
        setField(Integer.valueOf(words), 1);
        setField(Integer.valueOf(headlines), 2);
        setField(Integer.valueOf(outLinks), 3);
        setField(Double.valueOf(avgLinkDistance), 4);
        setField(Double.valueOf(outLinksPerWords), 5);
    }

    public ArticleTuple() {
    }
}
