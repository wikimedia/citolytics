package org.wikipedia.citolytics.cpa.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;

import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Check alphabetical order of WikiSim results.
 */
public class ValidateOrderInOutput extends WikiSimAbstractJob<Tuple3<String, String, Integer>> {
    public static void main(String[] args) throws Exception {
        new ValidateOrderInOutput().start(args);
    }

    public void plan() {
        outputFilename = args[1];
        result = env
                .readTextFile(args[0])
                .flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        String[] cols = Pattern.compile(Pattern.quote("|")).split(s);

                        String a = cols[1];
                        String b = cols[2];

                        if (a.compareTo(b) < 0)
                            out.collect(new Tuple3<>(a, b, 1));
                        else
                            out.collect(new Tuple3<>(b, a, 1));
                    }
                })
                .groupBy(0, 1)
                .reduceGroup(new GroupReduceFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<String, String, Integer>> in, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        Tuple3<String, String, Integer> rec = null;
                        Iterator<Tuple3<String, String, Integer>> iterator = in.iterator();

                        while (iterator.hasNext()) {
                            if (rec == null) {
                                rec = iterator.next();
                            } else {
                                rec.f2 += iterator.next().f2;
                            }
                        }

                        if (rec.f2 > 1)
                            out.collect(rec);
                    }
                });
    }
}
