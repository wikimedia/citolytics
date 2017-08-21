package org.wikipedia.citolytics.edits.types;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;

/**
 * Co-edits (articles that share the same authors). Map holds co-edited articles and number of co-edits.
 */
public class CoEditMap extends Tuple2<String, Map<String, Integer>> {
    public CoEditMap() {
        // empty constructor
    }

    public CoEditMap(String article, Map<String, Integer> coEdits) {
        f0 = article;
        f1 = coEdits;
    }

//    public List<WikiSimComparableResult<Double>> getList() {
//        return f1;
//    }
    public Map<String, Integer> getMap() {
        return f1;
    }


    public String getArticle() {
        return f0;
    }
}
