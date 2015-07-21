package de.tuberlin.dima.schubotz.wikisim.redirects;

import de.tuberlin.dima.schubotz.wikisim.cpa.utils.WikiSimConfiguration;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;
import java.util.regex.Pattern;


public class WikiSimRedirects {
    public static boolean debug = false;
    public static DataSet<WikiSimRedirectsResult2> wikiSimDataSet = null;

    // set up the execution environment
    public static void main(String[] args) throws Exception {

        if (args.length <= 2) {
            System.err.print("Error: Parameter missing! Required: [WIKISIM] [REDIRECTS] [OUTPUT], Current: " + Arrays.toString(args));
            System.exit(1);
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String outputFilename = args[2];

        // fields
        int hash = 0;
        int pageA = 1;
        int pageB = 2;
        int redirectSource = 0;
        int redirectTarget = 1;

        DataSet<Tuple2<String, String>> redirects = getRedirectsDataSet(env, args[1]);

        DataSet<WikiSimRedirectsResult2> res = getWikiSimDataSet(env, args[0])
                // replace page names with redirect target
                // page A
                .coGroup(redirects)
                .where(pageA)
                .equalTo(redirectSource)
                .with(new ReplaceRedirectsSingle(pageA))
                        // page B

                .coGroup(redirects)
                .where(pageB)
                .equalTo(redirectSource)
                .with(new ReplaceRedirectsSingle(pageB))
                        // sum duplicated tuples
                .groupBy(hash)
                .reduceGroup(new ReduceResultsSingle());

        if (outputFilename.equals("print")) {
            res.print();
        } else {
            res.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, String.valueOf(WikiSimConfiguration.csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("WikiSimRedirects");
    }

    public static DataSet<WikiSimRedirectsResult2> getWikiSimDataSet(ExecutionEnvironment env, String filename) {
        if (debug && filename.equals("dataset") && wikiSimDataSet != null) {
            return wikiSimDataSet;
        } else {
            return env.readTextFile(filename)
                    .map(new MapFunction<String, WikiSimRedirectsResult2>() {
                        @Override
                        public WikiSimRedirectsResult2 map(String s) throws Exception {
                            return new WikiSimRedirectsResult2(s);
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
