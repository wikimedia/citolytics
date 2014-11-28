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
package de.tuberlin.dima.schubotz.cpa.contracts;

import de.tuberlin.dima.schubotz.cpa.types.DataTypes;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;

@Combinable
public class calculateCPA extends RichGroupReduceFunction<DataTypes.Result, DataTypes.Result> {

    private long reducerThreshold;
    private long combinerThreshold;

    private Double alpha;
    private boolean calculateMedian = false;

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);

        alpha = parameter.getDouble("alpha", 1.5);
        reducerThreshold = parameter.getLong("reducerThreshold", 1);
        combinerThreshold = parameter.getLong("combinerThreshold", 0);
        calculateMedian = parameter.getBoolean("median", false);
    }

    @Override
    public void reduce(Iterable<DataTypes.Result> results, Collector<DataTypes.Result> resultCollector) throws Exception {
        internalReduce(results, resultCollector, reducerThreshold);
    }

    @Override
    public void combine(Iterable<DataTypes.Result> results, Collector<DataTypes.Result> resultCollector) throws Exception {
        internalReduce(results, resultCollector, combinerThreshold);
    }

    public void internalReduce(Iterable<DataTypes.Result> results, Collector<DataTypes.Result> resultCollector, long minOut) throws Exception {
        Iterator<DataTypes.Result> iterator = results.iterator();
        DataTypes.Result res = null;

        // Set default values
        long cnt = 0;
        long distance = 0;
        long min = Long.MAX_VALUE; //Integer.MAX_VALUE;
        long max = 0;
        long distSquared = 0;
        double recDistα = 0; //new BigDecimal(0);
        //DataTypes.ResultList distList = new DataTypes.ResultList();

        // Loop all record that belong to the given input key
        while (iterator.hasNext()) {
            res = iterator.next();

            // Fetch record fields
            long d = res.getDistance(); // distance
            long c = res.getCount(); //getField(2); // count

            // Increase total count, total distance
            distance += d;
            cnt += c;

            // Set min/max of distance
            //DataTypes.ResultList currentRl = res.getField(7);

            // Record already reduced?
            if (res.getDistSquared() > 0) {
                distSquared += res.getDistSquared(); //(Long) res.getField(3);
                recDistα += res.getCPA(); //(Double) res.getField(4);
                min = Math.min(min, res.getMin()); //(Integer) res.getField(5));
                max = Math.max(max, res.getMax()); //(Integer) res.getField(6));

                //distList = currentRl; //rec.getField(7, IntListValue.class); // Use existing distList
            } else {
                min = Math.min(min, d);
                max = Math.max(max, d);
                distSquared += d * d;
                //recDistα.add(new BigDecimal(Math.pow(d, alpha)));
                recDistα += Math.pow(d, alpha);

                // Add distance to list
                //distList.add(new IntValue(d));
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


//            res.setField(distance, 1);
//
//            res.setField(cnt, 2);
//            res.setField(distSquared, 3);
//            res.setField(recDistα, 4);
//
//            res.setField(min, 5);
//            res.setField(max, 6);

            if (calculateMedian) {
                //res.setField(distList, 7);
                //res.setField(getMedian(distList), 8);
            }

            resultCollector.collect(res);
        }

    }

    public static double getMedian(DataTypes.ResultList listv) {
        //listv.toArray(v); // NOT WORKING - Bug?
        Integer[] v = new Integer[listv.size()];
        int i = 0;

        for (IntValue vv : listv) {
            v[i] = vv.getValue();
            i++;
        }

        Arrays.sort(v);

        if (v.length == 0) {
            return 0;
        } else if (v.length == 1) {
            return v[0];
        } else {
            int middle = v.length / 2;
            if (v.length % 2 == 1) {
                return v[middle];
            } else {
                return (v[middle - 1] + v[middle]) / 2.0;
            }
        }
    }
}