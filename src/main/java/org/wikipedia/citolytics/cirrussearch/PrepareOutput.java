package org.wikipedia.citolytics.cirrussearch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.ClickStreamEvaluation;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

/**
 * Preparing WikiSim output to be added to Elasticsearch, i.e. transform to JSON.
 * <p/>
 * TODO Use ES directly as sink?
 *
 * @author malteschwarzer
 */
public class PrepareOutput extends WikiSimAbstractJob<Tuple1<String>> {

    public static void main(String[] args) throws Exception {
        new PrepareOutput().start(args);
    }

    @Override
    public void plan() throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String wikiSimInputFilename = params.getRequired("input");
        outputFilename = params.getRequired("output");

        int topK = params.getInt("topk", 10);
        int fieldScore = params.getInt("score", 5);
        int fieldPageA = params.getInt("page-a", 1);
        int fieldPageB = params.getInt("page-b", 2);
        boolean disableScores = params.has("disable-scores");


        setJobName("Cirrussearch PrepareOutput");

        DataSet<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimData = ClickStreamEvaluation.readWikiSimOutput(env, wikiSimInputFilename, topK, fieldPageA, fieldPageB, fieldScore);

        result = wikiSimData.map(new JSONMapper(disableScores));
    }

    class JSONMapper implements MapFunction<Tuple2<String, WikiSimComparableResultList<Double>>, Tuple1<String>> {
        private boolean disableScores = false;

        public JSONMapper() {

        }

        /**
         * @param disableScores Set to true if score should not be included in JSON output
         */
        public JSONMapper(boolean disableScores) {
            this.disableScores = disableScores;
        }

        @Override
        public Tuple1<String> map(Tuple2<String, WikiSimComparableResultList<Double>> in) throws Exception {

            ObjectMapper m = new ObjectMapper();
            ObjectNode n = m.createObjectNode();

            n.put("name", in.f0);
            n.put("namespace", 0);
            ArrayNode a = n.putArray("related_content");

            for (WikiSimComparableResult<Double> result : in.f1) {

                ObjectNode r = a.addObject();
                r.put("title", result.getName());

                if (!disableScores)
                    r.put("score", result.getSortField1());
            }

            return new Tuple1<>(n.toString()); // TODO Better way to serialize?
        }
    }
}
