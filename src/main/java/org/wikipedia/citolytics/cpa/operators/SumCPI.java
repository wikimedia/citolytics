package org.wikipedia.citolytics.cpa.operators;

import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;

import java.util.ArrayList;
import java.util.List;

public class SumCPI extends RichReduceFunction<RecommendationPair> {

    private double[] cpi_alpha;

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);

        String[] arr = parameter.getString("cpi_alpha", "1.5").split(",");
        cpi_alpha = new double[arr.length];
        for (int i = 0; i < arr.length; i++) {
            cpi_alpha[i] = Double.parseDouble(arr[i]);
        }

    }

    public List<Double> initCPI(long distance) {
//        DoubleListValue cpi = new DoubleListValue();
        List<Double> cpi = new ArrayList<>();
        for (double alpha : cpi_alpha) {
            cpi.add(Math.pow(distance, -alpha));
        }
        return cpi;
    }

    @Override
    public RecommendationPair reduce(RecommendationPair a, RecommendationPair b) throws Exception {
        // Reduce sums only -> use alpha already in flatMap
        RecommendationPair c = new RecommendationPair(a.getPageA(), a.getPageB());

        c.setDistance(a.getDistance() + b.getDistance());
        c.setCount(a.getCount() + b.getCount());

//        if (a.getCPI().size() == 0) {
//            // TODO Can this happen anyway?
//            a.setCPI(initCPI(a.getDistance()));
//        }
//        if (b.getCPI().size() == 0) {
//            b.setCPI(initCPI(b.getDistance()));
//        }

        c.setCPI(RecommendationPair.sum(a.getCPI(), b.getCPI()));

        return c;
    }
}