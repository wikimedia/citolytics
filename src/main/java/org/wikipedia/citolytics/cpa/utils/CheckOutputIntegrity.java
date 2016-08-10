package org.wikipedia.citolytics.cpa.utils;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Checks if two result sets are identical. Outputs varying records.
 * <p/>
 * USAGE: <test-input-a> <test-input-b> <output>
 */
public class CheckOutputIntegrity extends WikiSimAbstractJob<Tuple3<Long, String, String>> {
    public static void main(String[] args) throws Exception {
        new CheckOutputIntegrity().start(args);
    }

    @Override
    public void plan() {

        outputFilename = args[2];

        enableSingleOutputFile();

        DataSet<Tuple3<Long, Integer, String>> inputA = env.readTextFile(args[0])
                .flatMap(new ReadWikiSimResult());

        DataSet<Tuple3<Long, Integer, String>> inputB = env.readTextFile(args[1])
                .flatMap(new ReadWikiSimResult());


        result = inputA
                .coGroup(inputB)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple3<Long, Integer, String>, Tuple3<Long, Integer, String>, Tuple3<Long, String, String>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<Long, Integer, String>> inA, Iterable<Tuple3<Long, Integer, String>> inB, Collector<Tuple3<Long, String, String>> out) throws Exception {
                        long hash;

                        String vA = "-NULL-",
                                vB = "-NULL-";

                        int countA = -1,
                                countB = -1;

                        Iterator<Tuple3<Long, Integer, String>> iteratorA = inA.iterator();
                        Iterator<Tuple3<Long, Integer, String>> iteratorB = inB.iterator();

                        Tuple3<Long, Integer, String> a, b = null;

                        if (iteratorA.hasNext() && iteratorB.hasNext()) {
                            a = iteratorA.next();
                            b = iteratorB.next();

                            hash = a.f0;
                            vA = a.f2;
                            vB = b.f2;

                            countA = a.f1;
                            countB = b.f1;

                        } else if (iteratorA.hasNext() && !iteratorB.hasNext()) {
                            a = iteratorA.next();
                            hash = a.f0;
                            vA = a.f2;
                        } else if (!iteratorA.hasNext() && iteratorB.hasNext()) {
                            b = iteratorB.next();
                            hash = b.f0;
                            vB = b.f2;
                        } else {
                            throw new Exception("Both iterators empty.");
                        }


//                        if (!vA.equals(vB)) {
                        if (countA != countB) {
                            out.collect(new Tuple3<>(
                                    hash, vA, vB
                            ));
                        }
                    }
                });

    }

    class ReadWikiSimResult implements FlatMapFunction<String, Tuple3<Long, Integer, String>> {
        @Override
        public void flatMap(String s, Collector<Tuple3<Long, Integer, String>> out) throws Exception {
            String[] cols = Pattern.compile(Pattern.quote("|")).split(s);

            if (cols.length < 2 || cols[0].isEmpty() || cols[1].isEmpty())
                return;

            out.collect(new Tuple3<>(
                    Long.valueOf(cols[0]),
                    Integer.valueOf(cols[4]),
                    Arrays.toString(Arrays.copyOfRange(cols, 1, cols.length))
            ));
        }
    }
}
