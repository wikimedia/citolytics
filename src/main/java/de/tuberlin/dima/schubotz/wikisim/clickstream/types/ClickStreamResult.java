package de.tuberlin.dima.schubotz.wikisim.clickstream.types;

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
}
