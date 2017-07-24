package org.wikipedia.citolytics.clickstream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.operators.EvaluateClicks;
import org.wikipedia.citolytics.clickstream.types.ClickStreamRecommendationResult;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;
import org.wikipedia.citolytics.clickstream.utils.ClickStreamHelper;
import org.wikipedia.citolytics.cpa.io.WikiOutputFormat;
import org.wikipedia.citolytics.cpa.io.WikiSimReader;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.seealso.operators.MLTInputMapper;

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

    private boolean mltResults = false;
    public static DataSet<Tuple2<String, HashSet<String>>> links;

    public static void main(String[] args) throws Exception {
        new ClickStreamEvaluation().start(args);
    }


    public void init() {

        wikiSimInputFilename = getParams().getRequired("wikisim");
        clickStreamInputFilename = getParams().getRequired("gold");
        outputFilename = getParams().getRequired("output");

        langLinksInputFilename = getParams().get("langlinks");
        lang = getParams().get("lang");
        summary = getParams().has("summary");
        cpiExpr = getParams().get("cpi");
        articleStatsFilename = getParams().get("article-stats");
        idTitleMappingFilename = getParams().get("id-title-mapping");
        topRecommendationsFilename = getParams().get("top-recommendations");

    }

    public void plan() throws Exception {

        // Name
        setJobName("ClickStreamEvaluation");

        // Load gold standard (include translations with requested, provide id-title-mapping if non-id format is used)
        DataSet<ClickStreamTuple> clickStreamDataSet =
                ClickStreamHelper.getTranslatedClickStreamDataSet(env, clickStreamInputFilename, lang,
                        langLinksInputFilename, idTitleMappingFilename);

        // WikiSim
        DataSet<RecommendationSet> recommendationSets;
        Configuration config = WikiSimReader.getConfigFromArgs(getParams());

        int topK = getParams().getInt("topk", WikiSimConfiguration.DEFAULT_TOP_K);

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
        result = recommendationSets
                .coGroup(clickStreamDataSet)
                .where(RecommendationSet.SOURCE_TITLE_KEY)
                .equalTo(ClickStreamTuple.ARTICLE_NAME_KEY)
                .with(new EvaluateClicks(topK));

        // Top recommended articles (only #1 recommendations)
        // TODO limit out
        if(topRecommendationsFilename != null) {
            saveTopRecommendations(env, result, topRecommendationsFilename);
        }

        // Summarize results if requested
        if(summary) {
            summarize(ClickStreamResult.getSummaryFields());
        }
    }

    private static void saveTopRecommendations(ExecutionEnvironment env, DataSet<ClickStreamResult> result, String topRecommendationsFilename) throws Exception {
        DataSet<Tuple2<String, Long>> topRecommendations = result.flatMap(new FlatMapFunction<ClickStreamResult, Tuple2<String, Long>>() {
            @Override
            public void flatMap(ClickStreamResult t, Collector<Tuple2<String, Long>> out) throws Exception {
                if (t.getRecommendationsCount() > 0) {
                    out.collect(new Tuple2<>(t.getTopRecommendations(), 1L));
                }
            }
        })
                .groupBy(0)
                .sum(1)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> a, Tuple2<String, Long> b) throws Exception {
                        // Keep article name
                        return a.f1 > b.f1 ? a : b;
                    }
                });

        // Distinct recommendations
        DataSet<Tuple2<String, Long>> distinctRecommendations = result.flatMap(new FlatMapFunction<ClickStreamResult, Tuple2<String, Long>>() {
            @Override
            public void flatMap(ClickStreamResult clickStreamResult, Collector<Tuple2<String, Long>> out) throws Exception {
                for (ClickStreamRecommendationResult r : clickStreamResult.getRecommendations()) {
                    out.collect(new Tuple2<>(r.getRecommendedArticle(), 1L));
                }

            }
        }).distinct(0)
                .sum(1)
                .map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> in) throws Exception {
                        in.setField("Distinct recommendations", 0);
                        return in;
                    }
                });

        DataSet<Tuple2<String, Long>> count = env.fromElements(new Tuple2<String, Long>(
                "Article count", result.count())
        );

        topRecommendations = topRecommendations
                .union(distinctRecommendations)
                .union(count);

        topRecommendations
                .write(new WikiOutputFormat<>(topRecommendationsFilename), topRecommendationsFilename, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
    }
}
