package org.wikipedia.citolytics.seealso;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiSimReader;
import org.wikipedia.citolytics.cpa.types.Recommendation;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.seealso.operators.*;
import org.wikipedia.citolytics.seealso.types.SeeAlsoEvaluationResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Pattern;

/**
 * Flink job for running a "See also"-based evaluation on CPA or MLT result sets.
 * <p/>
 * Arguments:
 * 0 = RESULT-SET: path to results of CPA/CoCit or MLT
 * 1 = OUTPUT: filename of results (HDFS or print)
 * 2 = SEEALSO: path to extracted "See also"-links (output of wikisim.seealso.SeeAlsoExtractor)
 * 3 = LINKS-SET: path to extracted wiki links for filtering existing links (output of wikisim.linkgraph.LinksExtractor)
 * 4 = SCORE-FIELD: column of score field in result set (default: 6)
 * 5 = PAGE-A-FIELD: column of page A in result set (default: 1)
 * 6 = PAGE-B-FIELD: column of page B in result set (default: 2)
 * 7 = ENABLE-MRR: if set, performance measure is MRR (default: MAP)
 * <p/>
 * Set SCORE-FIELD = -1 for MLT result data set.
 */
public class SeeAlsoEvaluation extends WikiSimAbstractJob<SeeAlsoEvaluationResult> {
    public static String seeAlsoInputFilename;
    public static String wikiSimInputFilename;
    public static String linksInputFilename;

    public static DataSet<Tuple2<String, HashSet<String>>> links;

    public static void main(String[] args) throws Exception {
        new SeeAlsoEvaluation().start(args);
    }

    public void plan() throws Exception {

        setJobName("SeeAlso Evaluation");

        wikiSimInputFilename = getParams().getRequired("wikisim");
        outputFilename = getParams().getRequired("output");
        seeAlsoInputFilename = getParams().getRequired("gold");
        linksInputFilename = getParams().get("links", "nofilter");

        int scoreField = getParams().getInt("score", RecommendationPair.CPI_LIST_KEY);
        int fieldPageA = getParams().getInt("page-a", RecommendationPair.PAGE_A_KEY);
        int fieldPageB = getParams().getInt("page-b", RecommendationPair.PAGE_B_KEY);
        int fieldPageIdA = getParams().getInt("page-id-a", RecommendationPair.PAGE_A_ID_KEY);
        int fieldPageIdB = getParams().getInt("page-id-b", RecommendationPair.PAGE_B_ID_KEY);

        boolean enableMRR = getParams().has("enable-mrr");
        int topK = getParams().getInt("topk", WikiSimConfiguration.DEFAULT_TOP_K);

        // See also
        DataSet<Tuple2<String, ArrayList<String>>> seeAlsoDataSet = env.readTextFile(seeAlsoInputFilename)
                .map(new SeeAlsoInputMapper())
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple2<String, ArrayList<String>>>() {
                    @Override
                    public Tuple2<String, ArrayList<String>> reduce(Tuple2<String, ArrayList<String>> a, Tuple2<String, ArrayList<String>> b) throws Exception {
                        a.f1.addAll(b.f1);

                        return a;
                    }
                });

        // Read result set
        DataSet<RecommendationSet> wikiSimGroupedDataSet;

        // CPA or MLT results?
        if (scoreField >= 0 && fieldPageA >= 0 && fieldPageB >= 0) {
            // CPA
            jobName += " CPA score = " + scoreField + "; pages = " + fieldPageA + "; " + fieldPageB;
            Configuration config = new Configuration();

            config.setInteger("fieldPageA", fieldPageA);
            config.setInteger("fieldPageB", fieldPageB);
            config.setInteger("fieldPageIdA", fieldPageIdA);
            config.setInteger("fieldPageIdB", fieldPageIdB);
            config.setInteger("fieldScore", scoreField);

            DataSet<Recommendation> wikiSimDataSet = WikiSimReader.readWikiSimOutput(env, wikiSimInputFilename, config);

            // LinkFilter
            if (!linksInputFilename.isEmpty() && !linksInputFilename.equals("nofilter")) {
                wikiSimDataSet = wikiSimDataSet
                        .coGroup(getLinkDataSet(env, linksInputFilename))
                        .where(0)
                        .equalTo(0)
                        .with(new LinkExistsFilter());

                seeAlsoDataSet = seeAlsoDataSet
                        .coGroup(getLinkDataSet(env, linksInputFilename))
                        .where(0)
                        .equalTo(0)
                        .with(new SeeAlsoLinkExistsFilter());
            }

            wikiSimGroupedDataSet = wikiSimDataSet
                    .groupBy(0)
                    .reduceGroup(new RecommendationSetBuilder(topK));


        } else {
            // MLT
            jobName += " MLT";
            Configuration config = new Configuration();
            config.setInteger("topK", topK);

            wikiSimGroupedDataSet = env.readTextFile(wikiSimInputFilename)
                    .flatMap(new MLTInputMapper())
                    .withParameters(config);
        }

        // Evaluation
        result = seeAlsoDataSet
                .coGroup(wikiSimGroupedDataSet)
                .where(0)
                .equalTo(0)
                .with(new EvaluateSeeAlso(topK, enableMRR));
    }


    public static DataSet<Tuple2<String, HashSet<String>>> getLinkDataSet(ExecutionEnvironment env, String filename) {
        if (links == null) {
            links = env.readTextFile(filename)

                    .map(new MapFunction<String, Tuple2<String, HashSet<String>>>() {
                        Pattern delimiter = Pattern.compile(Pattern.quote("|"));

                        @Override
                        public Tuple2<String, HashSet<String>> map(String in) throws Exception {
                            String[] cols = delimiter.split(in);
                            return new Tuple2<>(
                                    cols[0],
                                    new HashSet<>(Arrays.asList(cols[1]))
                            );
                        }
                    })
                    .groupBy(0)
                    .reduce(new ReduceFunction<Tuple2<String, HashSet<String>>>() {
                        @Override
                        public Tuple2<String, HashSet<String>> reduce(Tuple2<String, HashSet<String>> a, Tuple2<String, HashSet<String>> b) throws Exception {
                            HashSet<String> set = (HashSet) a.getField(1);
                            set.addAll((HashSet) b.getField(1));
                            return new Tuple2<>(
                                    (String) a.getField(0),
                                    set);
                        }
                    });
        }

        return links;
    }
}
