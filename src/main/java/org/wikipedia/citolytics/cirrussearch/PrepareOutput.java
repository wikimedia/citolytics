package org.wikipedia.citolytics.cirrussearch;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.ClickStreamEvaluation;
import org.wikipedia.citolytics.cpa.WikiSim;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;
import org.wikipedia.citolytics.seealso.better.WikiSimGroupReducer;
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

        String wikiSimInputFilename = params.getRequired("wikisim");
        String wikiDumpInputFilename = params.getRequired("wikidump");

        outputFilename = params.getRequired("output");

        int topK = params.getInt("topk", 10);
        int fieldScore = params.getInt("score", 5);
        int fieldPageA = params.getInt("page-a", 1);
        int fieldPageB = params.getInt("page-b", 2);
        boolean disableScores = params.has("disable-scores");


        setJobName("Cirrussearch PrepareOutput");

//        DataSet<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimData =
//                ClickStreamEvaluation.readWikiSimOutput(env, wikiSimInputFilename, topK, fieldPageA, fieldPageB, fieldScore);

//        result = wikiSimData.map(new JSONMapper(disableScores));

        // TODO idMapping
        WikiSim wikiSimJob = new WikiSim();
        wikiSimJob.alpha = "0.9";
        wikiSimJob.inputFilename = wikiDumpInputFilename;
        wikiSimJob.setEnvironment(env);
        wikiSimJob.plan();

        // Build result list
        DataSet<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimData = wikiSimJob.result
                .flatMap(new FlatMapFunction<WikiSimResult, Tuple3<String, String, Double>>() {
                    private final int alphaKey = 0;
                    @Override
                    public void flatMap(WikiSimResult in, Collector<Tuple3<String, String, Double>> out) throws Exception {

                        out.collect(new Tuple3<>(in.getPageA(), in.getPageB(), in.getCPI(alphaKey)));
                        out.collect(new Tuple3<>(in.getPageB(), in.getPageA(), in.getCPI(alphaKey)));
                    }
                })
                .groupBy(0)
                .reduceGroup(new WikiSimGroupReducer(topK));

        // Transform result list to JSON with page ids
        result = wikiSimData.join(IdTitleMappingExtractor.extractIdTitleMapping(env, wikiDumpInputFilename))
                .where(0)
                .equalTo(1)
                .with(new JSONMapper(disableScores));
    }

//    public static Tuple1<String> prepareOutput(ExecutionEnvironment env, String wikiDumpInputFilename, boolean disableScores) {
//
//    }

    public

    class JSONMapper implements JoinFunction<Tuple2<String, WikiSimComparableResultList<Double>>, Tuple2<Integer, String>, Tuple1<String>> {
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
        public Tuple1<String> join(Tuple2<String, WikiSimComparableResultList<Double>> in, Tuple2<Integer, String> mapping) throws Exception {

            ObjectMapper m = new ObjectMapper();
            ObjectNode n = m.createObjectNode();

            n.put("id", mapping.f0);
            n.put("title", in.f0);
//            n.put("namespace", 0);
            ArrayNode a = n.putArray("citolytics");

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
