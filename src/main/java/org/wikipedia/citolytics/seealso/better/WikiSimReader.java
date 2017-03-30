package org.wikipedia.citolytics.seealso.better;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.WikiSimRecommendation;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;
import org.wikipedia.citolytics.cpa.types.WikiSimTopResults;

import java.util.Arrays;
import java.util.regex.Pattern;

public class WikiSimReader extends RichFlatMapFunction<String, WikiSimRecommendation> {
    int fieldScore = 9;
    int fieldPageA = 1;
    int fieldPageB = 2;
    int fieldPageIdA = WikiSimResult.PAGE_A_ID_KEY;
    int fieldPageIdB = WikiSimResult.PAGE_B_ID_KEY;

    private final int MAX_SCORE_LENGTH = 16;
    private final Pattern delimiterPattern = Pattern.compile(Pattern.quote("|"));

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);

        fieldScore = parameter.getInteger("fieldScore", 8);
        fieldPageA = parameter.getInteger("fieldPageA", 1);
        fieldPageB = parameter.getInteger("fieldPageB", 2);
    }

    @Override
    public void flatMap(String s, Collector<WikiSimRecommendation> out) throws Exception {
        String[] cols = delimiterPattern.split(s);

//        cols[fieldScore] = String.valueOf(100 * Math.random()).substring(0, 6);
//        System.out.println(cols[fieldScore]);

        if (fieldScore >= cols.length || fieldPageA >= cols.length || fieldPageB >= cols.length) {
            System.err.println("invalid col length : " + cols.length + " (score=" + fieldScore + ", a=" + fieldPageA + ", b=" + fieldPageB + "// " + s);
            return;
        }


        try {
            String scoreString = cols[fieldScore];
//            int maxLength = (scoreString.length() < MAX_SCORE_LENGTH) ? scoreString.length() : MAX_SCORE_LENGTH;
            Double score = Double.valueOf(scoreString);
//                    .substring(0, maxLength));

            out.collect(new WikiSimRecommendation(cols[fieldPageA], cols[fieldPageB], score, Integer.valueOf(cols[fieldPageIdA]), Integer.valueOf(cols[fieldPageIdB])));
            out.collect(new WikiSimRecommendation(cols[fieldPageB], cols[fieldPageA], score, Integer.valueOf(cols[fieldPageIdB]), Integer.valueOf(cols[fieldPageIdA])));
//        } catch (ArrayIndexOutOfBoundsException e) {
//            // nothing
//            return;
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Score field = " + fieldScore + "; cols length = " + cols.length + "; Raw = " + s + "\nArray =" + Arrays.toString(cols) + "\n" + e.getMessage());

        }
    }

    public static DataSet<WikiSimTopResults> readWikiSimOutput(ExecutionEnvironment env, String filename, int topK, int fieldPageA, int fieldPageB, int fieldScore) {

        Log.info("Reading WikiSim from " + filename);

        Configuration config = new Configuration();

        config.setInteger("fieldPageA", fieldPageA);
        config.setInteger("fieldPageB", fieldPageB);
        config.setInteger("fieldScore", fieldScore);

        return env.readTextFile(filename)
                .flatMap(new WikiSimReader())
                .withParameters(config)
                .groupBy(0)
                .reduceGroup(new WikiSimGroupReducer(topK));
    }
}
