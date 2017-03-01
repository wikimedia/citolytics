package org.wikipedia.citolytics.cpa.types;

import org.apache.flink.api.java.tuple.Tuple2;

public class IdTitleMapping extends Tuple2<Integer, String> {
    public IdTitleMapping() {
        // Flink requires empty constructor
    }

    public IdTitleMapping(Integer id, String title) {
        this.f0 = id;
        this.f1 = title;
    }

}
