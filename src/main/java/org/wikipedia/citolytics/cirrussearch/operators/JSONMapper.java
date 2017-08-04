package org.wikipedia.citolytics.cirrussearch.operators;

/**
 * Created by malte on 27.07.17.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

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
    public JSONMapper(boolean disableScores, boolean elasticBulkSyntax, boolean ignoreMissingIds, boolean includeIds) {
        this.disableScores = disableScores;
        this.elasticBulkSyntax = elasticBulkSyntax;
        this.ignoreMissingIds = ignoreMissingIds;
        this.includeIds = includeIds;
    }

    public void putResultArray(ObjectNode d, WikiSimComparableResultList<Double> results) throws Exception {
        ArrayNode a = d.putArray("citolytics");

        for (WikiSimComparableResult<Double> result : results) {

            ObjectNode r = a.addObject();
            r.put("title", result.getName());

            if (includeIds)
                r.put("id", String.valueOf(result.getId()));

            if (!disableScores) {
                // Test for too large scores (avoid Infinity values)
                if(result.getSortField1() > Integer.MAX_VALUE)
                    throw new Exception("Recommendation score > Integer.MAX_VALUE: " + result.getSortField1());

                r.put("score", result.getSortField1());
            }

        }
    }

    /**
     * Returns default JSON output. More minimalistic but needs to be transformed (by PySpark script)
     * before can be send to ES.
     *
     * @param pageId Page Id of source article
     * @param title Title of source article
     * @param results Recommendations
     * @return
     */
    protected String getDefaultOutput(int pageId, String title, WikiSimComparableResultList<Double> results) throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectNode n = m.createObjectNode();

        n.put("id", String.valueOf(pageId));
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
    protected String getElasticBulkOutput(int pageId, WikiSimComparableResultList<Double> results) throws Exception {

        ObjectMapper m = new ObjectMapper();
        ObjectNode a = m.createObjectNode();
        ObjectNode u = a.putObject("update");
        u.put("_id", String.valueOf(pageId));

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