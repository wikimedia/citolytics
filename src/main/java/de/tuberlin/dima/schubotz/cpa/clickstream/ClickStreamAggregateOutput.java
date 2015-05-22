package de.tuberlin.dima.schubotz.cpa.clickstream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;

public class ClickStreamAggregateOutput implements ReduceFunction<Tuple10<String, ArrayList<Tuple4<String, Double, Integer, Double>>, Integer, Integer, Integer, Double, Integer, Double, Integer, Double>> {
    @Override
    public Tuple10<String, ArrayList<Tuple4<String, Double, Integer, Double>>, Integer, Integer, Integer, Double, Integer, Double, Integer, Double> reduce(Tuple10<String, ArrayList<Tuple4<String, Double, Integer, Double>>, Integer, Integer, Integer, Double, Integer, Double, Integer, Double> a, Tuple10<String, ArrayList<Tuple4<String, Double, Integer, Double>>, Integer, Integer, Integer, Double, Integer, Double, Integer, Double> b) throws Exception {
        return new Tuple10<>(
                "SUM",
                new ArrayList<Tuple4<String, Double, Integer, Double>>(),
                ((Integer) a.getField(2)) + ((Integer) b.getField(2)),
                ((Integer) a.getField(3)) + ((Integer) b.getField(3)),
                ((Integer) a.getField(4)) + ((Integer) b.getField(4)),
                ((Double) a.getField(5)) + ((Double) b.getField(5)),
                ((Integer) a.getField(6)) + ((Integer) b.getField(6)),
                ((Double) a.getField(7)) + ((Double) b.getField(7)),
                ((Integer) a.getField(8)) + ((Integer) b.getField(8)),
                ((Double) a.getField(9)) + ((Double) b.getField(9))
        );
    }
}