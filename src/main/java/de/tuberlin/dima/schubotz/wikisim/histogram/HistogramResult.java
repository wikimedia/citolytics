package de.tuberlin.dima.schubotz.wikisim.histogram;

import org.apache.flink.api.java.tuple.Tuple4;

public class HistogramResult extends Tuple4<Integer, Integer, Integer, Long> {
    public HistogramResult() {
        setField(0, 0);
        setField(0, 1);
        setField(0, 2);
        setField(new Long(0), 3);
    }

    public HistogramResult(int ns, int articleCount, int linkCount, long linkPairCount) {
        setField(ns, 0);
        setField(articleCount, 1);
        setField(linkCount, 2);
        setField(linkPairCount, 3);
    }
}
