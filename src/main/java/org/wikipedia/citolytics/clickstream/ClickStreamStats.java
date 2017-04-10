package org.wikipedia.citolytics.clickstream;

import com.google.common.collect.Iterators;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;
import org.wikipedia.citolytics.clickstream.utils.ClickStreamHelper;

/**
 * Flink Job for testing clickstream dataset.
 */
public class ClickStreamStats extends WikiSimAbstractJob<Tuple2<Integer, Integer>> {
    public static void main(String[] args) throws Exception {
        new ClickStreamStats().start(args);
    }

    @Override
    public void plan() throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        outputFilename = params.getRequired("output");
        result = ClickStreamHelper.getClickStreamDataSet(env, params.getRequired("input"))
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<ClickStreamTuple, Tuple2<Integer, Integer>>() {
                    @Override
                    public void reduce(Iterable<ClickStreamTuple> in, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(1, Iterators.size(in.iterator())));
                    }
                })
                .aggregate(Aggregations.SUM, 0)
                .and(Aggregations.SUM, 1);
    }
}
