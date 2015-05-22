package de.tuberlin.dima.schubotz.cpa.redirects;

import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;
import java.util.regex.Pattern;


public class WikiSimRedirects {
    public static boolean debug = false;
    public static DataSet<WikiSimRedirectResult> wikiSimDataSet = null;

    // set up the execution environment
    public static void main(String[] args) throws Exception {

        if (args.length <= 2) {
            System.err.print("Error: Parameter missing! Required: [WIKISIM] [REDIRECTS] [OUTPUT], Current: " + Arrays.toString(args));
            System.exit(1);
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String outputFilename = args[2];

        int hash = 0;
        int pageA = 1;
        int pageB = 2;
        int redirectSource = 0;
        int redirectTarget = 1;

        DataSet<Tuple2<String, String>> redirects = getRedirectsDataSet(env, args[1]);

        DataSet<WikiSimRedirectResult> res = getWikiSimDataSet(env, args[0]);

//        DataSet<WikiSimRedirectResult> res = wikiSim
//                // replace page names with redirect target
//                // page A
//                .coGroup(redirects)
//                .where(pageA)
//                .equalTo(redirectSource)
//                .with(new ReplaceRedirects(pageA))
//                        // page B
//
//                .coGroup(redirects)
//                .where(pageB)
//                .equalTo(redirectSource)
//                .with(new ReplaceRedirects(pageB))
//                        // sum duplicated tuples
//                .groupBy(hash)
//                .reduceGroup(new ReduceResults());

        if (outputFilename.equals("print")) {
            res.print();
        } else {
            res.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, String.valueOf(WikiSimConfiguration.csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("WikiSimRedirects");
    }

    public static DataSet<WikiSimRedirectResult> getWikiSimDataSet(ExecutionEnvironment env, String filename) {
        if (debug && filename.equals("dataset") && wikiSimDataSet != null) {
            return wikiSimDataSet;
        } else {
            return env.readTextFile(filename)
                    .map(new MapFunction<String, WikiSimRedirectResult>() {
                        @Override
                        public WikiSimRedirectResult map(String s) throws Exception {
                            return new WikiSimRedirectResult(s);
                        }
                    });
        }
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
