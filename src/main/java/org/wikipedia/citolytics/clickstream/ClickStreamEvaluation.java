package org.wikipedia.citolytics.clickstream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.operators.EvaluateClicks;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;
import org.wikipedia.citolytics.clickstream.utils.ClickStreamHelper;
import org.wikipedia.citolytics.cpa.io.WikiOutputFormat;
import org.wikipedia.citolytics.cpa.io.WikiSimReader;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.seealso.better.MLTInputMapper;

import java.util.HashSet;

public class ClickStreamEvaluation extends WikiSimAbstractJob<ClickStreamResult> {
    public static String clickStreamInputFilename;
    public static String wikiSimInputFilename;
    public static String linksInputFilename;
    public static String outputAggregateFilename;
    public static String articleStatsFilename;

    private static String topRecommendationsFilename;
    private static String idTitleMappingFilename;
    private static String langLinksInputFilename = null;
    private static String lang = null;
    private static boolean summary = false;
    private static String cpiExpr;
    private static int fieldScore;
    private static int fieldPageA;
    private static int fieldPageB;

    private int topK = 10;
    private boolean mltResults = false;
    public static DataSet<Tuple2<String, HashSet<String>>> links;

    public static void main(String[] args) throws Exception {
        new ClickStreamEvaluation().start(args);
    }


    public void init() {

        ParameterTool params = ParameterTool.fromArgs(args);

        wikiSimInputFilename = params.getRequired("wikisim");
        clickStreamInputFilename = params.getRequired("gold");
        outputFilename = params.getRequired("output");
        topK = params.getInt("topk", 10);

        langLinksInputFilename = params.get("langlinks");
        lang = params.get("lang");
        summary = params.has("summary");
        cpiExpr = params.get("cpi");
        articleStatsFilename = params.get("article-stats");
        idTitleMappingFilename = params.get("id-title-mapping");
        topRecommendationsFilename = params.get("top-recommendations");

        fieldScore = params.getInt("score", 5);
        fieldPageA = params.getInt("page-a", 1);
        fieldPageB = params.getInt("page-b", 2);
    }

    public void plan() throws Exception {


        // Name
        setJobName("ClickStreamEvaluation");

        // Load gold standard (include translations with requested, provide id-title-mapping if non-id format is used)
        DataSet<ClickStreamTuple> clickStreamDataSet =
                ClickStreamHelper.getTranslatedClickStreamDataSet(env, clickStreamInputFilename, lang,
                        langLinksInputFilename, idTitleMappingFilename);

        // WikiSim
        DataSet<RecommendationSet> wikiSimGroupedDataSet;

        // CPA or MLT results?
        if (fieldScore >= 0 && fieldPageA >= 0 && fieldPageB >= 0) {
            // CPA
            jobName += " CPA Score=" + fieldScore + "; Page=[" + fieldPageA + ";" + fieldPageB + "]";

            wikiSimGroupedDataSet = WikiSimReader.buildRecommendationSets(env,
                    WikiSimReader.readWikiSimOutput(env, wikiSimInputFilename,
                    fieldPageA, fieldPageB, fieldScore), topK, cpiExpr, articleStatsFilename);

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
        result = wikiSimGroupedDataSet
                .coGroup(clickStreamDataSet)
                .where(RecommendationSet.SOURCE_TITLE_KEY)
                .equalTo(ClickStreamTuple.ARTICLE_NAME_KEY)
                .with(new EvaluateClicks(topK));

        // Top recommended articles (only #1 recommendations)
        // TODO limit out
        if(topRecommendationsFilename != null) {
            DataSet<Tuple2<String, Integer>> topRecommendations = result.flatMap(new FlatMapFunction<ClickStreamResult, Tuple2<String, Integer>>() {
                        @Override
                        public void flatMap(ClickStreamResult r, Collector<Tuple2<String, Integer>> out) throws Exception {
                            if (r.getRecommendationsCount() > 0) {
                                if(r.getRecommendations().get(0).getRecommendedArticle().equalsIgnoreCase("United States")) {
                                    out.collect(new Tuple2<>(r.getRecommendations().get(0).getRecommendedArticle(),
                                            1));
                                }
                            }
                        }
                    })
//                    .groupBy(0)
                    .sum(1)
//                    .
                    ;

            topRecommendations.write(new WikiOutputFormat<>(topRecommendationsFilename), topRecommendationsFilename, FileSystem.WriteMode.OVERWRITE);
        }


        // Summarize results if requested
        if(summary) {
            enableSingleOutputFile();

            result = result.sum(ClickStreamResult.IMPRESSIONS_KEY)
                    .andSum(ClickStreamResult.CLICKS_KEY)
                    .andSum(ClickStreamResult.CLICKS_K1_KEY)
                    .andSum(ClickStreamResult.CLICKS_K2_KEY)
                    .andSum(ClickStreamResult.CLICKS_K3_KEY)
                    .andSum(ClickStreamResult.RECOMMENDATIONS_COUNT_KEY);
        }
    }


}
