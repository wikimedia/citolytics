package org.wikipedia.citolytics.seealso;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiSimReader;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.seealso.operators.EvaluateSeeAlso;
import org.wikipedia.citolytics.seealso.operators.MLTInputMapper;
import org.wikipedia.citolytics.seealso.operators.SeeAlsoInputMapper;
import org.wikipedia.citolytics.seealso.types.SeeAlsoEvaluationResult;
import org.wikipedia.citolytics.seealso.types.SeeAlsoLinks;

import java.util.HashSet;

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
    public static String articleStatsFilename;

    private static String cpiExpr;
    private static boolean summary = false;

    public static DataSet<Tuple2<String, HashSet<String>>> links;

    public static void main(String[] args) throws Exception {
        new SeeAlsoEvaluation().start(args);
    }

    public void init() {
        wikiSimInputFilename = getParams().getRequired("wikisim");
        outputFilename = getParams().getRequired("output");
        seeAlsoInputFilename = getParams().getRequired("gold");
        cpiExpr = getParams().get("cpi");
        articleStatsFilename = getParams().get("article-stats");
        summary = getParams().has("summary");
    }

    public void plan() throws Exception {

        setJobName("SeeAlso Evaluation");

        int topK = getParams().getInt("topk", WikiSimConfiguration.DEFAULT_TOP_K);

        // See also
        DataSet<SeeAlsoLinks> seeAlsoDataSet = readSeeAlsoDataSet(env, seeAlsoInputFilename)
                .groupBy(0)
                .reduce(new ReduceFunction<SeeAlsoLinks>() {
                    @Override
                    public SeeAlsoLinks reduce(SeeAlsoLinks a, SeeAlsoLinks b) throws Exception {
                        // Merge duplicates in "See also" links
                        a.merge(b);

                        return a;
                    }
                });

        // Read result set
        DataSet<RecommendationSet> recommendationSets;
        Configuration config = WikiSimReader.getConfigFromArgs(getParams());

        // CPA or MLT results?
        if (getParams().has("mlt")) {
            // MLT
            jobName += " MLT";
            recommendationSets = env.readTextFile(wikiSimInputFilename)
                    .flatMap(new MLTInputMapper())
                    .withParameters(config);

        } else {
            // CPA
            jobName += " CPA";

            recommendationSets = WikiSimReader.buildRecommendationSets(env,
                    WikiSimReader.readWikiSimOutput(env, wikiSimInputFilename, config),
                    topK, cpiExpr, articleStatsFilename, false);
        }

        // Evaluation
        result = seeAlsoDataSet
                .coGroup(recommendationSets)
                .where(0)
                .equalTo(0)
                .with(new EvaluateSeeAlso(topK));

        // Summarize results if requested
        if(summary) {
            summarize(SeeAlsoEvaluationResult.getSummaryFields());
        }
    }

    /**
     * Read multiple "See also" data sets (comma separated). Merge is required.
     *
     * @param env
     * @param path
     * @return
     */
    private static DataSet<SeeAlsoLinks> readSeeAlsoDataSet(ExecutionEnvironment env, String path) {
        String[] paths = path.split(",");

        if(paths.length == 1) {
            return env.readTextFile(path).map(new SeeAlsoInputMapper());
        } else {
            DataSet<SeeAlsoLinks> dataSet = null;
            for(String p: paths) {
                if(dataSet == null) {
                    dataSet = env.readTextFile(path).map(new SeeAlsoInputMapper());
                } else {
                    dataSet = dataSet.union(env.readTextFile(path).map(new SeeAlsoInputMapper()));
                }
            }
            return dataSet;
        }
    }
}
