package org.wikipedia.citolytics.cpa.types;

import org.apache.flink.api.java.tuple.Tuple2;

public class RedirectMapping extends Tuple2<String, String> {
    public RedirectMapping() {
        // Flink requires empty constructor
    }

    public RedirectMapping(String source, String target) {
        f0 = source;
        f1 = target;
    }

    public String getSource() {
        return f0;
    }

    public String getTarget() {
        return f1;
    }

}
