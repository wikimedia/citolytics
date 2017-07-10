package org.wikipedia.citolytics.cpa.io;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.operators.ComputeComplexCPI;
import org.wikipedia.citolytics.cpa.types.Recommendation;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.seealso.operators.RecommendationSetBuilder;
import org.wikipedia.citolytics.stats.ArticleStats;
import org.wikipedia.citolytics.stats.ArticleStatsTuple;

import java.util.Arrays;
import java.util.regex.Pattern;

public class WikiSimReader extends RichFlatMapFunction<String, Recommendation> {
    private int fieldScore = RecommendationPair.CPI_LIST_KEY;
    private int fieldPageA = RecommendationPair.PAGE_A_KEY;
    private int fieldPageB = RecommendationPair.PAGE_B_KEY;
    private int fieldPageIdA = RecommendationPair.PAGE_A_ID_KEY;
    private int fieldPageIdB = RecommendationPair.PAGE_B_ID_KEY;

    private final Pattern delimiterPattern = Pattern.compile(Pattern.quote(WikiSimConfiguration.csvFieldDelimiter));

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);

        fieldScore = parameter.getInteger("fieldScore", fieldScore);
        fieldPageA = parameter.getInteger("fieldPageA", fieldPageA);
        fieldPageB = parameter.getInteger("fieldPageB", fieldPageB);
        fieldPageIdA = parameter.getInteger("fieldPageIdA", fieldPageIdA);
        fieldPageIdB = parameter.getInteger("fieldPageIdB", fieldPageIdB);

    }

    @Override
    public void flatMap(String s, Collector<Recommendation> out) throws Exception {
        String[] cols = delimiterPattern.split(s);

        // fieldScore >= cols.length ||
        if (fieldPageA >= cols.length || fieldPageB >= cols.length) {
            throw new Exception("invalid col length : " + cols.length + " (score=" + fieldScore + ", a=" + fieldPageA + ", b=" + fieldPageB + "// " + s);
//            return;
        }

        try {
//            String scoreString = cols[RecommendationPair.CPI_LIST_KEY];
            String scoreString = cols[fieldScore];


            // Create recommendations pair from cols
            RecommendationPair pair = new RecommendationPair(cols[fieldPageA], cols[fieldPageB]);
            pair.setPageAId(Integer.valueOf(cols[fieldPageIdA]));
            pair.setPageBId(Integer.valueOf(cols[fieldPageIdB]));
            pair.setCPI(Arrays.asList(Double.valueOf(scoreString)));
//            pair.setCPI(scoreString);

            collectRecommendationsFromPair(pair, out, false, 0);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Score field = " + fieldScore + "; cols length = " + cols.length + "; Raw = " + s + "\nArray =" + Arrays.toString(cols) + "\n" + e.getMessage());
        }
    }

    /**
     * From a single recommendation pair make two recommendations (A->B and B->A)
     *
     * @param pair Full recommendations pair (A, B in alphabetical order)
     * @param out
     */
    public static void collectRecommendationsFromPair(RecommendationPair pair, Collector<Recommendation> out, boolean backupRecommendations, int alphaKey) {
        out.collect(new Recommendation(pair.getPageA(), pair.getPageB(), pair.getCPI(alphaKey), pair.getPageAId(), pair.getPageBId()));

        // If pair is a backup recommendations, do not return full pair.
        if(!backupRecommendations || pair.getCPI(0) > WikiSimConfiguration.BACKUP_RECOMMENDATION_OFFSET) {
            out.collect(new Recommendation(pair.getPageB(), pair.getPageA(), pair.getCPI(alphaKey), pair.getPageBId(), pair.getPageAId()));
        }

    }


    public static DataSet<Recommendation> readWikiSimOutput(ExecutionEnvironment env, String filename,
                                                            int fieldPageA, int fieldPageB, int fieldScore) throws Exception {

        Log.info("Reading WikiSim from " + filename + "; scoreField=" + fieldScore);

        Configuration config = new Configuration();

        config.setInteger("fieldPageA", fieldPageA);
        config.setInteger("fieldPageB", fieldPageB);
        config.setInteger("fieldScore", fieldScore);

        // Read recommendation from files
        return env.readTextFile(filename)
                .flatMap(new WikiSimReader())
                .withParameters(config);
    }

    public static DataSet<RecommendationSet> buildRecommendationSets(ExecutionEnvironment env,
                                                                     DataSet<Recommendation> recommendations,
                                                                     int topK, String cpiExpr, String articleStatsFilename,
                                                                     boolean backupRecommendations) throws Exception {
//        DataSet<ArticleStatsTuple> stats = ArticleStats.getArticleStatsFromFile(env, articleStatsFilename);

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

//        DataSet<RecommendationSet> incompleteSets = recommendationSets.filter(new RecommendationSetFilter(false, topK));
//        DataSet<RecommendationSet> completeSets = recommendationSets.filter(new RecommendationSetFilter(true, topK));
//
//        DataSet<Tuple1<String>> articlesWithoutSets = recommendations
//                .rightOuterJoin(stats, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
//                .where(Recommendation.RECOMMENDATION_TITLE_KEY)
//                .equalTo(ArticleStatsTuple.ARTICLE_NAME_KEY)
//                .with(new FlatJoinFunction<Recommendation, ArticleStatsTuple, Tuple1<String>>() {
//                    @Override
//                    public void join(Recommendation recommendation, ArticleStatsTuple stats, Collector<Tuple1<String>> out) throws Exception {
//                        if(recommendation == null && stats != null) {
//                            out.collect(new Tuple1<>(stats.getArticle()));
//                        }
//                    }
//                });

        return recommendationSets;
    }


}
