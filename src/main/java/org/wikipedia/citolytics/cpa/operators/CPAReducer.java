package org.wikipedia.citolytics.cpa.operators;

import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;
import org.wikipedia.citolytics.cpa.types.list.DoubleListValue;

public class CPAReducer extends RichReduceFunction<WikiSimResult> {

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

    @Override
    public WikiSimResult reduce(WikiSimResult a, WikiSimResult b) throws Exception {
//        System.out.println(a.getPageA() + " / " + a.getPageB() + " == " + b.getPageA() + " / " + a.getPageB());

        // Reduce sums only -> use alpha already in flatMap
        WikiSimResult c = new WikiSimResult(a.getPageA(), a.getPageB());

        c.setDistance(a.getDistance() + b.getDistance());
        c.setCount(a.getCount() + b.getCount());
        c.setCPI(DoubleListValue.sum(a.getCPI(), b.getCPI()));

//        System.out.println(c.getHash() + " // A (" + a.getPageA() + " / " + a.getPageB() + ") = B (" + b.getPageA() + " / "
//                + b.getPageB() + ") // " + a.getCount() + " + " + b.getCount() + " = " + c.getCount());

//        System.out.println(c);

        return c;
    }
}