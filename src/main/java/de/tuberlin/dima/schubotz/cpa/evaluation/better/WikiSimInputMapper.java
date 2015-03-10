package de.tuberlin.dima.schubotz.cpa.evaluation.better;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.regex.Pattern;

public class WikiSimInputMapper extends RichFlatMapFunction<String, Tuple3<String, String, Double>> {
    int fieldScore = 9;
    int fieldPageA = 1;
    int fieldPageB = 2;

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
    public void flatMap(String s, Collector<Tuple3<String, String, Double>> out) throws Exception {
        String[] cols = delimiterPattern.split(s);

//        cols[fieldScore] = String.valueOf(100 * Math.random()).substring(0, 6);
//        System.out.println(cols[fieldScore]);
        try {
            String scoreString = cols[fieldScore];
//            int maxLength = (scoreString.length() < MAX_SCORE_LENGTH) ? scoreString.length() : MAX_SCORE_LENGTH;
            Double score = Double.valueOf(scoreString);
//                    .substring(0, maxLength));

            out.collect(new Tuple3<>(cols[fieldPageA], cols[fieldPageB], score));
            out.collect(new Tuple3<>(cols[fieldPageB], cols[fieldPageA], score));
        } catch (Exception e) {
            throw new Exception(fieldScore + "; " + cols.length + " === " + s + "\n" + Arrays.toString(cols) + "\n" + e.getMessage());
        }
    }
}
