package org.wikipedia.citolytics.cirrussearch;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.WikiSim;
import org.wikipedia.citolytics.cpa.io.WikiSimReader;
import org.wikipedia.citolytics.cpa.types.IdTitleMapping;
import org.wikipedia.citolytics.cpa.types.Recommendation;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

/**
 * Preparing WikiSim output to be added to Elasticsearch, i.e. transform to ES bulk JSON.
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
        int fieldScore = getParams().getInt("score", WikiSimConfiguration.DEFAULT_FIELD_SCORE);
        int fieldPageA = getParams().getInt("page-a", WikiSimConfiguration.DEFAULT_FIELD_PAGE_A);
        int fieldPageB = getParams().getInt("page-b", WikiSimConfiguration.DEFAULT_FIELD_PAGE_B);
        boolean disableScores = getParams().has("disable-scores");
        boolean elasticBulkSyntax = getParams().has("enable-elastic");
        boolean ignoreMissingIds = getParams().has("ignore-missing-ids");
        boolean resolveRedirects = getParams().has("resolve-redirects");
        boolean includeIds = getParams().has("include-ids");
        boolean relativeProximity = getParams().has("relative-proximity");
        boolean backupRecommendations = getParams().has("backup-recommendations");

        setJobName("CirrusSearch PrepareOutput");

        // Prepare results
        DataSet<Recommendation>  recommendations;
        if(wikiDumpInputFilename != null) {

            // Build new result list
            WikiSim wikiSimJob = new WikiSim();
            wikiSimJob.alpha = "0.855"; // From JCDL paper: a1=0.81, a2=0.9 -> a_mean = 0.855
            wikiSimJob.inputFilename = wikiDumpInputFilename;
            wikiSimJob.redirectsFilename = redirectsFilename;
            wikiSimJob.removeMissingIds = !ignoreMissingIds; // Ensures that page ids exist
            wikiSimJob.resolveRedirects = resolveRedirects;
            wikiSimJob.relativeProximity = relativeProximity;
            wikiSimJob.backupRecommendations = backupRecommendations;
            wikiSimJob.setEnvironment(env);
            wikiSimJob.plan();

            Configuration config = new Configuration();
            config.setBoolean("backupRecommendations", backupRecommendations);

            recommendations = wikiSimJob.result
                    .flatMap(new RichFlatMapFunction<RecommendationPair, Recommendation>() {
                        private boolean backupRecommendations;

                        @Override
                        public void open(Configuration parameter) throws Exception {
                            super.open(parameter);
                            backupRecommendations = parameter.getBoolean("backupRecommendations", false);
                        }

                        @Override
                        public void flatMap(RecommendationPair pair, Collector<Recommendation> out) throws Exception {
                            WikiSimReader.collectRecommendationsFromPair(pair, out, backupRecommendations, 0);
                        }
                    }).withParameters(config);

        } else if(wikiSimInputFilename != null) {
            // Use existing result list;
            recommendations = WikiSimReader.readWikiSimOutput(env, wikiSimInputFilename, fieldPageA, fieldPageB, fieldScore);
        } else {
            throw new Exception("Either --wikidump or --wikisim parameter must be provided.");
        }

        // Compute recommendation sets
        DataSet<RecommendationSet> recommendationSets = WikiSimReader.buildRecommendationSets(env, recommendations,
                topK, cpiExpr, articleStatsFilename, backupRecommendations);

                // Transform result list to JSON with page ids
        // TODO Check if there some pages lost (left or equi-join)
//        result = wikiSimData.leftOuterJoin(idTitleMapping)
//                .where(0)
//                .equalTo(1)
//                .with(new JSONMapperWithIdCheck(disableScores, elasticBulkSyntax, ignoreMissingIds, includeIds));

        result = recommendationSets.flatMap(new JSONMapper(disableScores, elasticBulkSyntax, ignoreMissingIds, includeIds));

    }


    /**
     * ES bulk JSON format builder for recommendation sets.
     */
    public class JSONMapper implements FlatMapFunction<RecommendationSet, Tuple1<String>> {
        protected boolean disableScores = false;
        protected boolean elasticBulkSyntax = false;
        protected boolean ignoreMissingIds = false;
        protected boolean includeIds = false;

        /**
         * @param disableScores Set to true if score should not be included in JSON output
         */
        JSONMapper(boolean disableScores, boolean elasticBulkSyntax, boolean ignoreMissingIds, boolean includeIds) {
            this.disableScores = disableScores;
            this.elasticBulkSyntax = elasticBulkSyntax;
            this.ignoreMissingIds = ignoreMissingIds;
            this.includeIds = includeIds;
        }

        public void putResultArray(ObjectNode d, WikiSimComparableResultList<Double> results) {
            ArrayNode a = d.putArray("citolytics");

            for (WikiSimComparableResult<Double> result : results) {

                ObjectNode r = a.addObject();
                r.put("title", result.getName());

                if (includeIds)
                    r.put("id", result.getId());

                if (!disableScores)
                    r.put("score", result.getSortField1());
            }
        }

        /**
         * Returns default JSON output. More minimalistic but needs to be transformed (by PySpark script)
         * before can be send to ES.
         *
         * @param pageId
         * @param title
         * @param results
         * @return
         */
        protected String getDefaultOutput(int pageId, String title, WikiSimComparableResultList<Double> results) {
            ObjectMapper m = new ObjectMapper();
            ObjectNode n = m.createObjectNode();

            n.put("id", pageId);
            n.put("title", title);
//            n.put("namespace", 0);
            putResultArray(n, results);

            return n.toString();
        }

        /**
         * Return JSON that is already in ES bulk syntax
         * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
         *
         * Example:
         * { "update" : {"_id" : "1", "_type" : "type1", "_index" : "index1", "_retry_on_conflict" : 3} }
         * { "doc" : {"field" : "value"} }
         *
         * @param pageId
         * @param results
         * @return
         */
        protected String getElasticBulkOutput(int pageId, WikiSimComparableResultList<Double> results) {

            ObjectMapper m = new ObjectMapper();
            ObjectNode a = m.createObjectNode();
            ObjectNode u = a.putObject("update");
            u.put("_id", pageId);

            ObjectNode s = m.createObjectNode();
            ObjectNode d = s.putObject("doc");

            putResultArray(d, results);

            return a.toString() + "\n" + s.toString(); // action_and_meta_data \n source
        }

        @Override
        public void flatMap(RecommendationSet in, Collector<Tuple1<String>> out) throws Exception {
            int pageId = in.getSourceId();

            // Check for page id
            if(!ignoreMissingIds && pageId < 0)
                return;

            out.collect(new Tuple1<>(elasticBulkSyntax ?
                    getElasticBulkOutput(pageId, in.getResults()) :
                    getDefaultOutput(pageId, in.getSourceTitle(), in.getResults())
            ));
        }
    }

    @Deprecated
    public class JSONMapperWithIdCheck extends JSONMapper implements FlatJoinFunction<RecommendationSet, IdTitleMapping, Tuple1<String>> {

        /**
         * @param disableScores     Set to true if score should not be included in JSON output
         * @param elasticBulkSyntax
         * @param ignoreMissingIds
         * @param includeIds
         */
        JSONMapperWithIdCheck(boolean disableScores, boolean elasticBulkSyntax, boolean ignoreMissingIds, boolean includeIds) {
            super(disableScores, elasticBulkSyntax, ignoreMissingIds, includeIds);
        }

        @Override
        public void join(RecommendationSet in, IdTitleMapping mapping, Collector<Tuple1<String>> out) throws Exception {
            int pageId;

            // Check for page id
            if(mapping == null) {
                if(ignoreMissingIds) {
                    pageId = -1; // Ignore missing page id
                } else {
                    return; // Don't ignore
                }
            } else {
                pageId = mapping.f0;
            }

            out.collect(new Tuple1<>(elasticBulkSyntax ?
                    getElasticBulkOutput(pageId, in.getResults()) :
                    getDefaultOutput(pageId, in.getSourceTitle(), in.getResults())
            ));
        }
    }
}
