package de.tuberlin.dima.schubotz.cpa.redirects;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;


public class MergeRedirects {
    public static boolean debug = false;

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length <= 2) {
            System.err.print("Error: Parameter missing! " + Arrays.toString(args));
            System.exit(1);
        }

        String outputFilename = args[2];

        int pageA = 1;
        int pageB = 2;
        int redirectSource = 0;
        int redirectTarget = 1;

        DataSet<Tuple2<String, String>> redirects = getRedirectsDataSet(env, args[1]);

        DataSet<WikiSimRedirectResult> wikiSim = getWikiSimDataSet(env, args[0]);

        DataSet<WikiSimRedirectResult> res = wikiSim
                // replace page names with redirect target
                // page A
                .coGroup(redirects)
                .where(pageA)
                .equalTo(redirectSource)
                .with(new ReplaceRedirects(pageA))
                        // page B

                .coGroup(redirects)
                .where(pageB)
                .equalTo(redirectSource)
                .with(new ReplaceRedirects(pageB))
                        // sum duplicated tuples
                .groupBy(pageA, pageB)
                .reduceGroup(new GroupReduceFunction<WikiSimRedirectResult, WikiSimRedirectResult>() {
                    @Override
                    public void reduce(Iterable<WikiSimRedirectResult> in, Collector<WikiSimRedirectResult> out) throws Exception {
                        Iterator<WikiSimRedirectResult> iterator = in.iterator();
                        WikiSimRedirectResult reducedRecord = null;

                        while (iterator.hasNext()) {
                            WikiSimRedirectResult currentRecord = iterator.next();

                            // init
                            if (reducedRecord == null) {
                                reducedRecord = currentRecord;
                            } else {
                                // sum
                                reducedRecord.sumWith(currentRecord);
                            }
                        }

                        out.collect(reducedRecord);
                    }
                });


        res.print();

        env.execute();
    }

    // -9223368383335529186|Mister Immortal|Jarrod Washburn|722|1|1.0|0.00483709781788956|0.004240471278688157
    public static DataSet<WikiSimRedirectResult> getWikiSimDataSet(ExecutionEnvironment env, String filename) {
//        if(debug) {
//            return env.fromElements(
//                    new Tuple3<>("A", "B", 1),
//                    new Tuple3<>("Aa", "B", 1),
//                    new Tuple3<>("C", "D", 1)
//            );
//        } else {
        return env.readTextFile(filename)
                .map(new MapFunction<String, WikiSimRedirectResult>() {
                    @Override
                    public WikiSimRedirectResult map(String s) throws Exception {
                        return new WikiSimRedirectResult(s);
                    }
                });
//        }
    }

    public static DataSet<Tuple2<String, String>> getRedirectsDataSet(ExecutionEnvironment env, String filename) {
        if (debug) {
            return env.fromElements(
                    new Tuple2<>("Aaa", "A"),
                    new Tuple2<>("Aa", "A"),
                    new Tuple2<>("Aaaa", "A"),
                    new Tuple2<>("D", "A")
            );
        } else {
            return env.readTextFile(filename)
                    .map(new MapFunction<String, Tuple2<String, String>>() {
                        @Override
                        public Tuple2<String, String> map(String s) throws Exception {
                            String[] cols = s.split(Pattern.quote("|"));
                            return new Tuple2<>(cols[0], cols[1]);
                        }
                    });
        }
    }

}
