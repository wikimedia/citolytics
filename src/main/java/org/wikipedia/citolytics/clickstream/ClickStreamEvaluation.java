package org.wikipedia.citolytics.clickstream;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.operators.EvaluateClicks;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;
import org.wikipedia.citolytics.clickstream.utils.ClickStreamHelper;
import org.wikipedia.citolytics.cpa.types.WikiSimTopResults;
import org.wikipedia.citolytics.seealso.better.MLTInputMapper;
import org.wikipedia.citolytics.seealso.better.WikiSimReader;

import java.util.HashSet;

public class ClickStreamEvaluation extends WikiSimAbstractJob<ClickStreamResult> {
    public static String clickStreamInputFilename;
    public static String wikiSimInputFilename;
    public static String linksInputFilename;
    public static String outputAggregateFilename;

    private static String langLinksInputFilename = null;
    private static String lang = null;
    private static boolean summary = false;

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

        langLinksInputFilename = params.get("langlinks");
        lang = params.get("lang");
        summary = params.has("summary");

        int fieldScore = params.getInt("score", 5);
        int fieldPageA = params.getInt("page-a", 1);
        int fieldPageB = params.getInt("page-b", 2);

        // Name
        setJobName("ClickStreamEvaluation");

        // Load gold standard (include translations with requested)
        DataSet<ClickStreamTuple> clickStreamDataSet =
                ClickStreamHelper.getTranslatedClickStreamDataSet(env, clickStreamInputFilename, lang, langLinksInputFilename);

        // WikiSim
        DataSet<WikiSimTopResults> wikiSimGroupedDataSet;

        // CPA or MLT results?
        if (fieldScore >= 0 && fieldPageA >= 0 && fieldPageB >= 0) {
            // CPA
            jobName += " CPA Score=" + fieldScore + "; Page=[" + fieldPageA + ";" + fieldPageB + "]";

            wikiSimGroupedDataSet = WikiSimReader.readWikiSimOutput(env, wikiSimInputFilename, topK, fieldPageA, fieldPageB, fieldScore);
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
                .where(WikiSimTopResults.SOURCE_TITLE_KEY)
                .equalTo(ClickStreamTuple.ARTICLE_NAME_KEY)
                .with(new EvaluateClicks(topK));

        // Summarize results if requested
        if(summary) {
            result = result.sum(ClickStreamResult.IMPRESSIONS_KEY)
                    .andSum(ClickStreamResult.CLICKS_KEY)
                    .andSum(ClickStreamResult.CLICKS_K1_KEY)
                    .andSum(ClickStreamResult.CLICKS_K2_KEY)
                    .andSum(ClickStreamResult.CLICKS_K3_KEY);
        }
    }


}
