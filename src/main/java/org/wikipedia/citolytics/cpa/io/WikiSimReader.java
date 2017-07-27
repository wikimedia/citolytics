package org.wikipedia.citolytics.cpa.io;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.operators.ComputeComplexCPI;
import org.wikipedia.citolytics.cpa.types.Recommendation;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.seealso.operators.RecommendationSetBuilder;
import org.wikipedia.citolytics.stats.ArticleStats;
import org.wikipedia.citolytics.stats.types.ArticleStatsTuple;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Reads WikiSim output from file and builds recommendation sets.
 */
public class WikiSimReader extends RichFlatMapFunction<String, Recommendation> {
    private int fieldScore = RecommendationPair.CPI_LIST_KEY;
    private int fieldPageA = RecommendationPair.PAGE_A_KEY;
    private int fieldPageB = RecommendationPair.PAGE_B_KEY;
    private int fieldPageIdA = RecommendationPair.PAGE_A_ID_KEY;
    private int fieldPageIdB = RecommendationPair.PAGE_B_ID_KEY;

    private boolean setPageIds = false;

    private final Pattern delimiterPattern = Pattern.compile(Pattern.quote(WikiSimConfiguration.csvFieldDelimiter));

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);

        fieldScore = parameter.getInteger("fieldScore", fieldScore);
        fieldPageA = parameter.getInteger("fieldPageA", fieldPageA);
        fieldPageB = parameter.getInteger("fieldPageB", fieldPageB);
        fieldPageIdA = parameter.getInteger("fieldPageIdA", fieldPageIdA);
        fieldPageIdB = parameter.getInteger("fieldPageIdB", fieldPageIdB);

        setPageIds = fieldPageIdA >= 0 && fieldPageIdB >= 0;
    }

    @Override
    public void flatMap(String s, Collector<Recommendation> out) throws Exception {
        String[] cols = delimiterPattern.split(s);

        try {
            String scoreString = cols[fieldScore];

            // Create recommendations pair from cols
            RecommendationPair pair = new RecommendationPair(cols[fieldPageA], cols[fieldPageB]);

            // Ignore page ids if requested
            if (setPageIds) {
                pair.setPageAId(Integer.valueOf(cols[fieldPageIdA]));
                pair.setPageBId(Integer.valueOf(cols[fieldPageIdB]));
            }

            // Use only single CPI value
            pair.setCPI(Arrays.asList(Double.valueOf(scoreString)));
//            pair.setCPI(scoreString);

            collectRecommendationsFromPair(pair, out, false, 0);

        } catch (Exception e) {
            throw new Exception("Cannot read WikiSim output. Score field = " + fieldScore + "; cols length = "
                    + cols.length + "; Raw = " + s + "\nArray =" + Arrays.toString(cols) + "\nException: " + e.getMessage());
        }
    }

    /**
     * From a single recommendation pair make two recommendations (A->B and B->A)
     *
     * @param pair Full recommendations pair (A, B in alphabetical order)
     * @param out Collector
     */
    public static void collectRecommendationsFromPair(RecommendationPair pair, Collector<Recommendation> out, boolean backupRecommendations, int alphaKey) {
        out.collect(new Recommendation(pair.getPageA(), pair.getPageB(), pair.getCPI(alphaKey), pair.getPageAId(), pair.getPageBId()));

        // If pair is a backup recommendations, do not return full pair.
        if(!backupRecommendations || pair.getCPI(0) > WikiSimConfiguration.BACKUP_RECOMMENDATION_OFFSET) {
            out.collect(new Recommendation(pair.getPageB(), pair.getPageA(), pair.getCPI(alphaKey), pair.getPageBId(), pair.getPageAId()));
        }

    }

    public static Configuration getConfigFromArgs(ParameterTool args) {
        Configuration config = new Configuration();

        config.setInteger("fieldPageA", args.getInt("page-a", RecommendationPair.PAGE_A_KEY));
        config.setInteger("fieldPageIdA", args.getInt("page-id-a", RecommendationPair.PAGE_A_ID_KEY));
        config.setInteger("fieldPageB", args.getInt("page-b", RecommendationPair.PAGE_B_KEY));
        config.setInteger("fieldPageIdB", args.getInt("page-id-b", RecommendationPair.PAGE_B_ID_KEY));
        config.setInteger("fieldScore", args.getInt("score", RecommendationPair.CPI_LIST_KEY));
        config.setInteger("topK", args.getInt("topk", WikiSimConfiguration.DEFAULT_TOP_K));

        return config;
    }

    /**
     * Reads WikiSim output from local
     *
     * @param env Environment
     * @param filename Path (HDFS or local)
     * @param config Configuration with fields etc.
     * @return Recommendation pairs
     * @throws Exception Invalid input
     */
    public static DataSet<Recommendation> readWikiSimOutput(ExecutionEnvironment env, String filename, Configuration config) throws Exception {
        Log.info("Reading WikiSim from " + filename);

        // Read recommendation from files
        return env.readTextFile(filename)
                .flatMap(new WikiSimReader())
                .withParameters(config);
    }

    public static DataSet<Recommendation> readWikiSimOutput(ExecutionEnvironment env, String filename,
                                                            int fieldPageA, int fieldPageB, int fieldScore, int fieldPageIdA, int fieldPageIdB) throws Exception {
        Configuration config = new Configuration();

        config.setInteger("fieldPageA", fieldPageA);
        config.setInteger("fieldPageIdA", fieldPageIdA);
        config.setInteger("fieldPageB", fieldPageB);
        config.setInteger("fieldPageIdB", fieldPageIdB);
        config.setInteger("fieldScore", fieldScore);

        return readWikiSimOutput(env, filename, config);
    }

    /**
     * Build recommendation sets from recommendation pairs.
     *
     * TODO Optimize this method for performance (currently bottleneck for evaluations)
     *
     * @param env Execution environment
     * @param recommendations Data set of recommendation pairs
     * @param topK Number of top recommendations included in set.
     * @param cpiExpr Complete complex CPI score (optional)
     * @param articleStatsFilename Path to article stats data set (if complex CPI enabled)
     * @param backupRecommendations Enable for backup recommendations
     * @return Recommendation set with top-k recommendations
     * @throws Exception If complex CPI failed
     */
    public static DataSet<RecommendationSet> buildRecommendationSets(ExecutionEnvironment env,
                                                                     DataSet<Recommendation> recommendations,
                                                                     int topK, String cpiExpr, String articleStatsFilename,
                                                                     boolean backupRecommendations) throws Exception {

        // Compute complex CPI with expression
        if(cpiExpr != null && articleStatsFilename != null) {
            // TODO redirects?
            DataSet<ArticleStatsTuple> stats = ArticleStats.getArticleStatsFromFile(env, articleStatsFilename);

            // Total articles
            long count = stats.count();

            // TODO JoinHint? Currently using left hybrid build second
            recommendations = recommendations
                    .leftOuterJoin(stats, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
                    .where(Recommendation.RECOMMENDATION_TITLE_KEY)
                    .equalTo(ArticleStatsTuple.ARTICLE_NAME_KEY)
                    .with(new ComputeComplexCPI(count, cpiExpr, backupRecommendations));
        }

        DataSet<RecommendationSet> recommendationSets = recommendations
                .groupBy(Recommendation.SOURCE_TITLE_KEY) // Using HashPartition Sort on [0; ASC] TODO Maybe use reduce()
                .reduceGroup(new RecommendationSetBuilder(topK));

        return recommendationSets;
    }


}
