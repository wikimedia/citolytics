package de.tuberlin.dima.schubotz.wikisim.clickstream;

import de.tuberlin.dima.schubotz.wikisim.WikiSimJob;
import de.tuberlin.dima.schubotz.wikisim.seealso.better.MLTInputMapper;
import de.tuberlin.dima.schubotz.wikisim.seealso.better.WikiSimGroupReducer;
import de.tuberlin.dima.schubotz.wikisim.seealso.better.WikiSimInputMapper;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResultList;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class ClickStreamEvaluation extends WikiSimJob<ClickStreamResult> {
    public static String clickStreamInputFilename;
    public static String wikiSimInputFilename;
    public static String linksInputFilename;
    public static String outputAggregateFilename;

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
                    .reduceGroup(new WikiSimGroupReducer(10));
        } else {
            // MLT
            jobName += " MLT";

            Configuration config = new Configuration();
            config.setInteger("topK", 10);

            wikiSimGroupedDataSet = env.readTextFile(wikiSimInputFilename)
                    .flatMap(new MLTInputMapper())
                    .withParameters(config);
        }

        // Evaluation
        result = wikiSimGroupedDataSet
                .coGroup(clickStreamDataSet)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<String, WikiSimComparableResultList<Double>>, Tuple3<String, Integer, HashMap<String, Integer>>, ClickStreamResult>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimRecords, Iterable<Tuple3<String, Integer, HashMap<String, Integer>>> clickStreamRecords, Collector<ClickStreamResult> out) throws Exception {
                        Iterator<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimIterator = wikiSimRecords.iterator();
                        Iterator<Tuple3<String, Integer, HashMap<String, Integer>>> clickStreamIterator = clickStreamRecords.iterator();

                        // Proceed only when both records exist
                        if (!wikiSimIterator.hasNext() || !clickStreamIterator.hasNext()) {
                            return;
                        }

                        Tuple2<String, WikiSimComparableResultList<Double>> wikiSimRecord = wikiSimIterator.next();

                        WikiSimComparableResultList<Double> retrievedDocuments = wikiSimRecord.f1;

                        ArrayList<Tuple3<String, Double, Integer>> results = new ArrayList<>();
                        int impressions = 0;
                        int outClicks = 0;
                        int[] k = new int[]{10, 5, 1};
                        int[] totalClicks = new int[]{0, 0, 0};

                        Tuple3<String, Integer, HashMap<String, Integer>> clickStreamRecord = clickStreamIterator.next();

                        HashMap<String, Integer> clickStream = clickStreamRecord.f2;
                        impressions = clickStreamRecord.f1;

                        int rank = 1;

                        // Clicks on retrieved docs
                        for (WikiSimComparableResult doc : retrievedDocuments) {
                            int clicks = clickStream.containsKey(doc.getName()) ? clickStream.get(doc.getName()) : 0;

                            results.add(new Tuple3<>(
                                    doc.getName(),
                                    (Double) doc.getSortField1(),
                                    clicks
                            ));

                            // loop k's
                            for (int i = 0; i < k.length; i++) {
                                if (rank <= k[i]) {
                                    totalClicks[i] += clicks;
                                }
                            }
                            rank++;
                        }

                        // All out clicks
                        for (Integer c : clickStream.values())
                            outClicks += c;


                        ClickStreamResult res = new ClickStreamResult();

                        res.f0 = wikiSimRecord.f0;
                        res.f1 = results;
                        res.f2 = results.size();
                        res.f3 = impressions;
                        res.f4 = outClicks;
                        res.f5 = totalClicks[0];
                        res.f6 = totalClicks[1];
                        res.f7 = totalClicks[2];

                        out.collect(
                                res
                        );

                    }
                });
    }
}
