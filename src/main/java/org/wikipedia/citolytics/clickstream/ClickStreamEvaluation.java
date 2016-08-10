package org.wikipedia.citolytics.clickstream;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.wikipedia.citolytics.WikiSimJob;
import org.wikipedia.citolytics.clickstream.operators.EvaluateClicks;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.clickstream.utils.ClickStreamHelper;
import org.wikipedia.citolytics.seealso.better.MLTInputMapper;
import org.wikipedia.citolytics.seealso.better.WikiSimGroupReducer;
import org.wikipedia.citolytics.seealso.better.WikiSimInputMapper;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

import java.util.HashMap;
import java.util.HashSet;

public class ClickStreamEvaluation extends WikiSimJob<ClickStreamResult> {
    public static String clickStreamInputFilename;
    public static String wikiSimInputFilename;
    public static String linksInputFilename;
    public static String outputAggregateFilename;

    private final int topK = 10;
    private boolean mltResults = false;
    public static DataSet<Tuple2<String, HashSet<String>>> links;

    public static void main(String[] args) throws Exception {
        new ClickStreamEvaluation().start(args);
    }

    public void plan() {
        wikiSimInputFilename = args[0];
        clickStreamInputFilename = args[1];
        outputFilename = args[2];

        int scoreField = (args.length > 3 ? Integer.valueOf(args[3]) : 5);
        int fieldPageA = (args.length > 4 ? Integer.valueOf(args[4]) : 1);
        int fieldPageB = (args.length > 5 ? Integer.valueOf(args[5]) : 2);

        // Name
        setJobName("ClickStreamEvaluation");

        // Gold standard
        DataSet<Tuple3<String, Integer, HashMap<String, Integer>>> clickStreamDataSet = ClickStreamHelper.getRichClickStreamDataSet(env, clickStreamInputFilename);

        // WikiSim
        DataSet<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimGroupedDataSet;

        // CPA or MLT results?
        if (scoreField >= 0 && fieldPageA >= 0 && fieldPageB >= 0) {
            // CPA
            jobName += " CPA Score=" + scoreField + "; Page=[" + fieldPageA + ";" + fieldPageB + "]";

            Configuration config = new Configuration();

            config.setInteger("fieldPageA", fieldPageA);
            config.setInteger("fieldPageB", fieldPageB);
            config.setInteger("fieldScore", scoreField);

            wikiSimGroupedDataSet = env.readTextFile(wikiSimInputFilename)
                    .flatMap(new WikiSimInputMapper())
                    .withParameters(config)
                    .groupBy(0)
                    .reduceGroup(new WikiSimGroupReducer(topK));
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
}
