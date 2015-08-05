/*        __
 *        \ \
 *   _   _ \ \  ______
 *  | | | | > \(  __  )
 *  | |_| |/ ^ \| || |
 *  | ._,_/_/ \_\_||_|
 *  | |
 *  |_|
 * 
 * ----------------------------------------------------------------------------
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <rob ∂ CLABS dot CC> wrote this file. As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.
 * ----------------------------------------------------------------------------
 */
package de.tuberlin.dima.schubotz.wikisim.cpa.operators;

import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiSimResult;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Calculates CPA values, CoCit strength, distance, min. distance, max. distance for each result record.
 */
@Combinable
public class CPAReducer extends RichGroupReduceFunction<WikiSimResult, WikiSimResult> {

    private int reducerThreshold;
    private int combinerThreshold;

    private double[] alpha;

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);

        String[] arr = parameter.getString("alpha", "1.5").split(",");
        alpha = new double[arr.length];
        for (int i = 0; i < arr.length; i++) {
            alpha[i] = Double.parseDouble(arr[i]);
        }

        reducerThreshold = parameter.getInteger("reducerThreshold", 0);
        combinerThreshold = parameter.getInteger("combinerThreshold", 0);
    }

    @Override
    public void reduce(Iterable<WikiSimResult> results, Collector<WikiSimResult> resultCollector) throws Exception {
        internalReduce(results, resultCollector, reducerThreshold);
    }

    @Override
    public void combine(Iterable<WikiSimResult> results, Collector<WikiSimResult> resultCollector) throws Exception {
        internalReduce(results, resultCollector, combinerThreshold);
    }

    public void internalReduce(Iterable<WikiSimResult> results, Collector<WikiSimResult> resultCollector, int minOut) throws Exception {
        Iterator<WikiSimResult> iterator = results.iterator();
        WikiSimResult res = null;

        // Set default values
        int cnt = 0;
        long distance = 0;
        long min = Long.MAX_VALUE;
        long max = 0;
        long distSquared = 0;
        double[] recDistα = new double[alpha.length];

        // Loop all record that belong to the given input key
        while (iterator.hasNext()) {
            res = iterator.next();

            // Fetch record fields
            long d = res.getDistance(); // distance
            int c = res.getCount(); //getField(2); // count

            // Increase total count, total distance
            distance += d;
            cnt += c;

            // Record already reduced?
            if (res.getCount() > 1) {
                distSquared += res.getDistSquared(); //(Long) res.getField(3);

                for (int i = 0; i < alpha.length; i++) {
                    recDistα[i] += res.getCPA(i); //(Double) res.getField(4);
                }
                //recDistα.add(res.getCPA());
                min = Math.min(min, res.getMin()); //(Integer) res.getField(5));
                max = Math.max(max, res.getMax()); //(Integer) res.getField(6));
            } else {
                min = Math.min(min, d);
                max = Math.max(max, d);
                distSquared += d * d;
                //recDistα.add(new WikiSimBigDecimal(new BigDecimal(Math.pow(d, alpha))));
                for (int i = 0; i < alpha.length; i++) {
                    recDistα[i] += Math.pow(d, -alpha[i]);
                }
            }
        }

        // Total count greater than threshold
        if (cnt > minOut) {
            assert res != null;

            // Set total values in record (stratosphere IntValue)
            res.setDistance(distance);
            res.setCount(cnt);
            res.setDistSquared(distSquared);
            res.setCPA(recDistα);
            res.setMin(min);
            res.setMax(max);

            resultCollector.collect(res);
        }

    }
}