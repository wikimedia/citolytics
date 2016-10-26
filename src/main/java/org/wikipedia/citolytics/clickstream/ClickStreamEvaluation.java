package org.wikipedia.citolytics.clickstream;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.operators.EvaluateClicks;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.clickstream.utils.ClickStreamHelper;
import org.wikipedia.citolytics.seealso.better.MLTInputMapper;
import org.wikipedia.citolytics.seealso.better.WikiSimGroupReducer;
import org.wikipedia.citolytics.seealso.better.WikiSimInputMapper;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

import java.util.HashMap;
import java.util.HashSet;

public class ClickStreamEvaluation extends WikiSimAbstractJob<ClickStreamResult> {
    public static String clickStreamInputFilename;
    public static String wikiSimInputFilename;
    public static String linksInputFilename;
    public static String outputAggregateFilename;

    private int topK = 10;
    private boolean mltResults = false;
    public static DataSet<Tuple2<String, HashSet<String>>> links;

    public static void main(String[] args) throws Exception {
        new ClickStreamEvaluation().start(args);
    }

    public void plan() {

        ParameterTool params = ParameterTool.fromArgs(args);

        wikiSimInputFilename = params.getRequired("wikisim");
        clickStreamInputFilename = params.getRequired("gold");
        outputFilename = params.getRequired("output");
        topK = params.getInt("topk", 10);

        int fieldScore = params.getInt("score", 5);
        int fieldPageA = params.getInt("page-a", 1);
        int fieldPageB = params.getInt("page-b", 2);

        // Name
        setJobName("ClickStreamEvaluation");

        // Gold standard
        DataSet<Tuple3<String, Integer, HashMap<String, Integer>>> clickStreamDataSet = ClickStreamHelper.getRichClickStreamDataSet(env, clickStreamInputFilename);

        // WikiSim
        DataSet<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimGroupedDataSet;

        // CPA or MLT results?
        if (fieldScore >= 0 && fieldPageA >= 0 && fieldPageB >= 0) {
            // CPA
            jobName += " CPA Score=" + fieldScore + "; Page=[" + fieldPageA + ";" + fieldPageB + "]";

            wikiSimGroupedDataSet = readWikiSimOutput(env, wikiSimInputFilename, topK, fieldPageA, fieldPageB, fieldScore);
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
                .where(0)
                .equalTo(0)
                .with(new EvaluateClicks(topK));
    }

    public static DataSet<Tuple2<String, WikiSimComparableResultList<Double>>> readWikiSimOutput(ExecutionEnvironment env, String filename, int topK, int fieldPageA, int fieldPageB, int fieldScore) {

        Log.info("Reading WikiSim from " + filename);

        Configuration config = new Configuration();

        config.setInteger("fieldPageA", fieldPageA);
        config.setInteger("fieldPageB", fieldPageB);
        config.setInteger("fieldScore", fieldScore);

        return env.readTextFile(filename)
                .flatMap(new WikiSimInputMapper())
                .withParameters(config)
                .groupBy(0)
                .reduceGroup(new WikiSimGroupReducer(topK));
    }
}
