package de.tuberlin.dima.schubotz.wikisim.cpa.utils;

import de.tuberlin.dima.schubotz.wikisim.WikiSimJob;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Checks if two result sets are identical. Outputs varying records.
 * <p/>
 * USAGE: <test-input-a> <test-input-b> <output>
 */
public class CheckOutputIntegrity extends WikiSimJob<Tuple3<Long, String, String>> {
    public static void main(String[] args) throws Exception {
        new CheckOutputIntegrity().start(args);
    }

    @Override
    public void plan() {

        outputFilename = args[2];

        DataSet<Tuple2<Long, String>> inputA = env.readTextFile(args[0])
                .flatMap(new ReadWikiSimResult());

        DataSet<Tuple2<Long, String>> inputB = env.readTextFile(args[1])
                .flatMap(new ReadWikiSimResult());


        result = inputA
                .coGroup(inputB)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<Long, String>, Tuple2<Long, String>, Tuple3<Long, String, String>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Long, String>> inA, Iterable<Tuple2<Long, String>> inB, Collector<Tuple3<Long, String, String>> out) throws Exception {
                        long hash;
                        String vA = "-NULL-";
                        String vB = "-NULL-";

                        Iterator<Tuple2<Long, String>> iteratorA = inA.iterator();
                        Iterator<Tuple2<Long, String>> iteratorB = inB.iterator();

                        Tuple2<Long, String> a, b = null;

                        if (iteratorA.hasNext() && iteratorB.hasNext()) {
                            a = iteratorA.next();
                            b = iteratorB.next();

                            System.out.println(a);

                            hash = a.f0;
                            vA = a.f1;
                            vB = b.f1;

                        } else if (iteratorA.hasNext() && !iteratorB.hasNext()) {
                            a = iteratorA.next();
                            hash = a.f0;
                            vA = a.f1;
                        } else if (!iteratorA.hasNext() && iteratorB.hasNext()) {
                            b = iteratorB.next();
                            hash = b.f0;
                            vB = b.f1;
                        } else {
                            throw new Exception("Both iterators empty.");
                        }


                        if (!vA.equals(vB)) {
                            out.collect(new Tuple3<>(
                                    hash, vA, vB
                            ));
                        }
                    }
                });

    }

    class ReadWikiSimResult implements FlatMapFunction<String, Tuple2<Long, String>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<Long, String>> out) throws Exception {
            String[] cols = Pattern.compile(Pattern.quote("|")).split(s, 2);

            if (cols.length < 2 || cols[0].isEmpty() || cols[1].isEmpty())
                return;

            out.collect(new Tuple2<>(
                    Long.valueOf(cols[0]), cols[1]
            ));
        }
    }
}
