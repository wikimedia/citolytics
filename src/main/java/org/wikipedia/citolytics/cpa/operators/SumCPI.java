package org.wikipedia.citolytics.cpa.operators;

import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.DoubleValue;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.types.list.DoubleListValue;

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

    public DoubleListValue initCPI(long distance) {
        DoubleListValue cpi = new DoubleListValue();
        for (double alpha : cpi_alpha) {
            cpi.add(new DoubleValue(Math.pow(distance, -alpha)));
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

        c.setCPI(DoubleListValue.sum(a.getCPI(), b.getCPI()));

        return c;
    }
}