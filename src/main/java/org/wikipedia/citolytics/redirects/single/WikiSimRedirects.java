package org.wikipedia.citolytics.redirects.single;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;

import java.util.regex.Pattern;

/**
 * Resolves redirects in WikiSim output for debug and testing purpose.
 * <p/>
 * INFO: Use WikiSim with redirects on the fly. (wikisim.cpa.WikiSim)
 */
public class WikiSimRedirects extends WikiSimAbstractJob<WikiSimRedirectsResult>{

    public static void main(String[] args) throws Exception {
        new WikiSimRedirects().start(args);
    }

    @Override
    public void plan() throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        outputFilename = params.getRequired("output");
        // fields
        int hash = 0;
        int pageA = 1;
        int pageB = 2;
        int redirectSource = 0;
        int redirectTarget = 1;

        DataSet<RedirectMapping> redirects = getRedirectsDataSet(env, params.getRequired("redirects"));

        result = getWikiSimDataSet(env, params.getRequired("wikisim"))
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

    }


    public static DataSet<WikiSimRedirectsResult> getWikiSimDataSet(ExecutionEnvironment env, String filename) {
        return env.readTextFile(filename)
            .map(new MapFunction<String, WikiSimRedirectsResult>() {
                @Override
                public WikiSimRedirectsResult map(String s) throws Exception {
                    return new WikiSimRedirectsResult(s);
                }
            });

    }

    public static DataSet<RedirectMapping> getRedirectsDataSet(ExecutionEnvironment env, String filename) {
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
