package org.wikipedia.citolytics.edits.types;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class CoEditList extends Tuple2<String, List<String>> {
    public CoEditList() {
        // empty constructor
    }

    public CoEditList(String article, List<String> coEdits) {
        f0 = article;
        f1 = coEdits;
    }

//    public List<WikiSimComparableResult<Double>> getList() {
//        return f1;
//    }
    public List<String> getList() {
        return f1;
    }


    public String getArticle() {
        return f0;
    }
}
