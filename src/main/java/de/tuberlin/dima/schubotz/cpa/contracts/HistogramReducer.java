package de.tuberlin.dima.schubotz.cpa.contracts;


import de.tuberlin.dima.schubotz.cpa.types.DataTypes.HistogramResult;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable;

import org.apache.flink.util.Collector;

import java.util.Iterator;


@Combinable
public class HistogramReducer extends RichGroupReduceFunction<HistogramResult, HistogramResult> {

    public void internalReduce(Iterable<HistogramResult> results, Collector<HistogramResult> resultCollector) throws Exception {
        Iterator<HistogramResult> iterator = results.iterator();
        HistogramResult res = null;

        int articleCount = 0;
        int linkCount = 0;
        long linkpairCount = 0;

        while (iterator.hasNext()) {
            res = iterator.next();

            articleCount += (int) res.getField(1);
            linkCount += (int) res.getField(2);
            linkpairCount += (long) res.getField(3);
        }

        if (res == null)
            return;

        res.setField(articleCount, 1);
        res.setField(linkCount, 2);
        res.setField(linkpairCount, 3);

        resultCollector.collect(res);
    }

    @Override
    public void reduce(Iterable<HistogramResult> results, Collector<HistogramResult> resultCollector) throws Exception {
        internalReduce(results, resultCollector);
    }

    @Override
    public void combine(Iterable<HistogramResult> results, Collector<HistogramResult> resultCollector) throws Exception {
        internalReduce(results, resultCollector);
    }
}
