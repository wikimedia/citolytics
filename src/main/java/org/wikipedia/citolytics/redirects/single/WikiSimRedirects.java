package org.wikipedia.citolytics.redirects.single;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;
import org.wikipedia.citolytics.cpa.utils.WikiSimOutputWriter;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Resolves redirects in WikiSim output for debug and testing purpose.
 * <p/>
 * INFO: Use WikiSim with redirects on the fly. (wikisim.cpa.WikiSim)
 */
public class WikiSimRedirects {
    public static boolean debug = false;
    public static DataSet<WikiSimRedirectsResult> wikiSimDataSet = null;

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

        DataSet<RedirectMapping> redirects = getRedirectsDataSet(env, args[1]);

        DataSet<WikiSimRedirectsResult> res = getWikiSimDataSet(env, args[0])
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

        new WikiSimOutputWriter<WikiSimRedirectsResult>("WikiSimRedirects (single)")
                .write(env, res, outputFilename);

    }

    public static DataSet<WikiSimRedirectsResult> getWikiSimDataSet(ExecutionEnvironment env, String filename) {
        if (debug && filename.equals("dataset") && wikiSimDataSet != null) {
            return wikiSimDataSet;
        } else {
            return env.readTextFile(filename)
                    .map(new MapFunction<String, WikiSimRedirectsResult>() {
                        @Override
                        public WikiSimRedirectsResult map(String s) throws Exception {
                            return new WikiSimRedirectsResult(s);
                        }
                    });
        }
    }

    public static DataSet<RedirectMapping> getRedirectsDataSet(ExecutionEnvironment env, String filename) {
        if (debug) {
            return env.fromElements(
                    new RedirectMapping("Aaa", "A"),
                    new RedirectMapping("Aa", "A"),
                    new RedirectMapping("Aaaa", "A"),
                    new RedirectMapping("D", "A")
            );
        } else {
            return env.readTextFile(filename)
                    .map(new MapFunction<String, RedirectMapping>() {
                        @Override
                        public RedirectMapping map(String s) throws Exception {
                            String[] cols = s.split(Pattern.quote("|"));
                            return new RedirectMapping(cols[0], cols[1]);
                        }
                    });
        }
    }

}
