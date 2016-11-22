package org.wikipedia.citolytics.clickstream.types;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;

import java.util.ArrayList;

/**
 * article | retrieved documents | number of ret. documents | impressions | sum out clicks | k=10 clicks | k=5 clicks | k=1 clicks
 * <p/>
 * OLD:
 * article | retrieved documents | number of ret. documents | counter | k=10 rel clicks | k=10 clicks | k=5 rel clicks | k=5 clicks | k=1 rel clicks | k=1 clicks
 */
public class ClickStreamResult extends
        //    Tuple10<String, ArrayList<Tuple4<String, Double, Integer, Double>>, Integer, Integer, Integer, Double, Integer, Double, Integer, Double>
        Tuple8<String, ArrayList<Tuple3<String, Double, Integer>>, Integer, Integer, Integer, Integer, Integer, Integer> {


    public ClickStreamResult() {
    }

    public ClickStreamResult(String article, ArrayList<Tuple3<String, Double, Integer>> results, int resultsCount,
                             int impressions, int totalClicks, int clicks1, int clicks2, int clicks3) {
        f0 = article;
        f1 = results;
        f2 = resultsCount;
        f3 = impressions;
        f4 = totalClicks;
        f5 = clicks1;
        f6 = clicks2;
        f7 = clicks3;
    }

    public String getArticle() {
        return f0;
    }

    public ArrayList<Tuple3<String, Double, Integer>> getResults() {
        return f1;
    }

    public int getImpressions() {
        return f3;
    }

    public int getTotalClicks() {
        return f4;
    }

    public int getClicks1() {
        return f5;
    }

    public int getClicks2() {
        return f6;
    }

    public int getClicks3() {
        return f7;
    }
}
