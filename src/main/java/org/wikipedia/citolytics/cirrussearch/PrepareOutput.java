package org.wikipedia.citolytics.cirrussearch;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cirrussearch.operators.BackupRecommendationSetExtractor;
import org.wikipedia.citolytics.cirrussearch.operators.BackupRecommendationSetMerger;
import org.wikipedia.citolytics.cirrussearch.operators.JSONMapper;
import org.wikipedia.citolytics.cpa.WikiSim;
import org.wikipedia.citolytics.cpa.io.WikiSimReader;
import org.wikipedia.citolytics.cpa.types.Recommendation;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;

/**
 * Preparing WikiSim output to be added to Elasticsearch, i.e. transform to ES bulk JSON.
 *
 * Example:
 * { "update" : {"_id" : "1" } }
 * { "doc" : {"citolytics" : [ { "title": "Foo", "score": 1.5 } ] } }
 * ...
 */
public class PrepareOutput extends WikiSimAbstractJob<Tuple1<String>> {

    public static void main(String[] args) throws Exception {
        new PrepareOutput().start(args);
    }

    @Override
    public void plan() throws Exception {
        String wikiSimInputFilename = getParams().get("wikisim"); // not in use

        String wikiDumpInputFilename = getParams().get("wikidump");
        String articleStatsFilename = getParams().get("article-stats");
        String cpiExpr = getParams().get("cpi");

        outputFilename = getParams().getRequired("output");
        String redirectsFilename = getParams().get("redirects", null);
        int topK = getParams().getInt("topk", WikiSimConfiguration.DEFAULT_TOP_K);
        int fieldScore = getParams().getInt("score", RecommendationPair.CPI_LIST_KEY);
        int fieldPageA = getParams().getInt("page-a", RecommendationPair.PAGE_A_KEY);
        int fieldPageB = getParams().getInt("page-b", RecommendationPair.PAGE_B_KEY);
        int fieldPageIdA = getParams().getInt("page-id-a", RecommendationPair.PAGE_A_ID_KEY);
        int fieldPageIdB = getParams().getInt("page-id-b", RecommendationPair.PAGE_B_ID_KEY);

        boolean disableScores = getParams().has("disable-scores");
        boolean elasticBulkSyntax = getParams().has("enable-elastic");
        boolean ignoreMissingIds = getParams().has("ignore-missing-ids");
        boolean resolveRedirects = getParams().has("resolve-redirects");
        boolean includeIds = getParams().has("include-ids");
        boolean relativeProximity = getParams().has("relative-proximity");
        boolean backupRecommendations = getParams().has("backup-recommendations");
        double alpha = getParams().getDouble("alpha", 0.855); // From JCDL paper: a1=0.81, a2=0.9 -> a_mean = 0.855

        setJobName("CirrusSearch PrepareOutput");

        // Prepare results
        DataSet<Recommendation>  recommendations;
        if(wikiSimInputFilename != null) {
            // Use existing result list;
            recommendations = WikiSimReader.readWikiSimOutput(env, wikiSimInputFilename, fieldPageA, fieldPageB, fieldScore, fieldPageIdA, fieldPageIdB);
        } else if(wikiDumpInputFilename != null) {

            // Build new result list
            WikiSim wikiSimJob = new WikiSim();
            wikiSimJob.alpha = String.valueOf(alpha);
            wikiSimJob.inputFilename = wikiDumpInputFilename;
            wikiSimJob.redirectsFilename = redirectsFilename;
            wikiSimJob.ignoreMissingIds = ignoreMissingIds; // Ensures that page ids exist
            wikiSimJob.resolveRedirects = resolveRedirects;
            wikiSimJob.relativeProximity = relativeProximity;
            wikiSimJob.setEnvironment(env);
            wikiSimJob.plan();

            recommendations = wikiSimJob.result
                    .flatMap(new RichFlatMapFunction<RecommendationPair, Recommendation>() {
                        @Override
                        public void flatMap(RecommendationPair pair, Collector<Recommendation> out) throws Exception {
                            WikiSimReader.collectRecommendationsFromPair(pair, out, 0);
                        }
                    });

        } else {
            throw new Exception("Either --wikidump or --wikisim parameter must be provided.");
        }

        // Compute recommendation sets
        DataSet<RecommendationSet> recommendationSets = WikiSimReader.buildRecommendationSets(env, recommendations,
                topK, cpiExpr, articleStatsFilename);

        // Backup recommendations
        if(backupRecommendations) {
            if(wikiDumpInputFilename == null) {
                throw new Exception("To use backup recommendations the --wikidump parameter must be provided.");
            }

            // Merge original and backup recommendation sets
            recommendationSets = recommendationSets
                    .fullOuterJoin(BackupRecommendationSetExtractor.getBackupRecommendations(env, wikiDumpInputFilename))
                    .where(RecommendationSet.SOURCE_TITLE_KEY)
                    .equalTo(RecommendationSet.SOURCE_TITLE_KEY)
                    .with(new BackupRecommendationSetMerger(topK));
        }

        // Transform result list to JSON
        result = recommendationSets.flatMap(new JSONMapper(disableScores, elasticBulkSyntax, ignoreMissingIds, includeIds));
    }
}
