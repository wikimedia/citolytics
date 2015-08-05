package de.tuberlin.dima.schubotz.wikisim.clickstream;

import de.tuberlin.dima.schubotz.wikisim.clickstream.utils.ClickStreamHelper;
import de.tuberlin.dima.schubotz.wikisim.cpa.utils.WikiSimOutputWriter;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.com.google.common.collect.Iterators;
import org.apache.flink.util.Collector;

/**
 * Flink Job for testing clickstream dataset.
 */
public class ClickStreamStats {
    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length < 2) {
            System.err.println("Parameters missing: INPUT OUTPUT");
            System.exit(1);
        }

        String outputFilename = args[1];

        // Count articles with ClickStream data, target links
        DataSet<Tuple2<Integer, Integer>> output = ClickStreamHelper.getClickStreamDataSet(env, args[0])
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple3<String, String, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<String, String, Integer>> in, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(1, Iterators.size(in.iterator())));
                    }
                })
                .aggregate(Aggregations.SUM, 0)
                .and(Aggregations.SUM, 1);


        new WikiSimOutputWriter<Tuple2<Integer, Integer>>("ClickStream Stats")
                .setParallelism(1)
                .write(env, output, outputFilename);

    }
}
