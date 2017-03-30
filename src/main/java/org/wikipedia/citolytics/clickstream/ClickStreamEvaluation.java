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

        int fieldScore = params.getInt("score", 5);
        int fieldPageA = params.getInt("page-a", 1);
        int fieldPageB = params.getInt("page-b", 2);

        // Name
        setJobName("ClickStreamEvaluation");

        // Gold standard

        // Multi language evaluation
//        if(lang != null && !lang.equalsIgnoreCase("en") && langLinksInputFilename != null && !langLinksInputFilename.isEmpty()) {
            DataSet<ClickStreamTuple> clickStreamDataSet = ClickStreamHelper.getTranslatedClickStreamDataSet(env, clickStreamInputFilename, lang, langLinksInputFilename);
//        }

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

        // Multi language evaluation
//        if(lang != null && !lang.equalsIgnoreCase("en") && langLinksInputFilename != null && !langLinksInputFilename.isEmpty()) {
//            // Load enwiki language links
//            DataSet<LangLinkTuple> langLinks = MultiLang.readLangLinksDataSet(env, langLinksInputFilename, lang);
//
//            clickStreamDataSet.join(langLinks)
//                    .where(ClickStreamTuple.ARTICLE_ID_KEY)
//                    .equalTo(LangLinkTuple.PAGE_ID_KEY)
//                    .with(new JoinFunction<ClickStreamTuple, LangLinkTuple, Object>() {
//                    }
//        }


        try {
            System.out.println("WIKISIM = ");
            wikiSimGroupedDataSet.print();
            System.out.println("CS = ....");
            clickStreamDataSet.print();

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Evaluation
        result = wikiSimGroupedDataSet
                .coGroup(clickStreamDataSet)
                .where(0) // source name
                .equalTo(0) // click stream article name
                .with(new EvaluateClicks(topK));
    }


}
