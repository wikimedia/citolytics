package de.tuberlin.dima.schubotz.cpa.clickstream;

import de.tuberlin.dima.schubotz.cpa.WikiSim;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.WikiSimGroupReducer;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.WikiSimInputMapper;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResultList;
import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.com.google.common.collect.Ordering;
import org.apache.flink.util.Collector;

import java.util.*;

public class ClickStreamEvaluation {
    public static String outputFilename;
    public static String clickStreamInputFilename;
    public static String wikiSimInputFilename;
    public static String linksInputFilename;
    public static String outputAggregateFilename;

    public static DataSet<Tuple2<String, HashSet<String>>> links;

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length <= 3) {
            System.err.println("Input/output parameters missing!");
            System.err.println(new WikiSim().getDescription());
            System.exit(1);
        }

        wikiSimInputFilename = args[0];
        clickStreamInputFilename = args[1];
        outputFilename = args[2];
        outputAggregateFilename = args[3];
        linksInputFilename = args[4];

        int scoreField = (args.length > 5 ? Integer.valueOf(args[5]) : 6);
        int fieldPageA = (args.length > 6 ? Integer.valueOf(args[6]) : 1);
        int fieldPageB = (args.length > 7 ? Integer.valueOf(args[7]) : 2);


        Configuration config = new Configuration();

        config.setInteger("fieldPageA", fieldPageA);
        config.setInteger("fieldPageB", fieldPageB);
        config.setInteger("fieldScore", scoreField);

        // Gold standard
        DataSet<Tuple3<String, String, Integer>> clickStreamDataSet = ClickStream.getClickStreamDataSet(env, clickStreamInputFilename);

        // WikiSim
        DataSet<Tuple3<String, String, Double>> wikiSimDataSet = env.readTextFile(wikiSimInputFilename)
                .flatMap(new WikiSimInputMapper())
                .withParameters(config);

        // LinkFilter
//        if (!linksInputFilename.isEmpty() && !linksInputFilename.equals("nofilter")) {
//            wikiSimDataSet = wikiSimDataSet
//                    .coGroup(BetterEvaluation.getLinkDataSet(env, linksInputFilename))
//                    .where(0)
//                    .equalTo(0)
//                    .with(new BetterLinkExistsFilter());
//        }

        DataSet<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimGroupedDataSet = wikiSimDataSet
                .groupBy(0)
                .reduceGroup(new WikiSimGroupReducer(10));

        // Evaluation
        /*
        Article | Retrieved documents | Number of retrieved documents | sum rel. clicks
                  - Doc A | score | clicks | rel. clicks
         */
        // article | retrieved documents | number of ret. documents | counter | k=10 rel clicks | k=10 clicks | k=5 rel clicks | k=5 clicks | k=1 rel clicks | k=1 clicks
        DataSet<Tuple10<String, ArrayList<Tuple4<String, Double, Integer, Double>>, Integer, Integer, Integer, Double, Integer, Double, Integer, Double>>
                output = wikiSimGroupedDataSet
                .coGroup(clickStreamDataSet)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<String, WikiSimComparableResultList<Double>>, Tuple3<String, String, Integer>, Tuple10<String, ArrayList<Tuple4<String, Double, Integer, Double>>, Integer, Integer, Integer, Double, Integer, Double, Integer, Double>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimRecords, Iterable<Tuple3<String, String, Integer>> clickStreams, Collector<Tuple10<String, ArrayList<Tuple4<String, Double, Integer, Double>>, Integer, Integer, Integer, Double, Integer, Double, Integer, Double>> out) throws Exception {
                        Iterator<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimIterator = wikiSimRecords.iterator();
                        Iterator<Tuple3<String, String, Integer>> clickStreamIterator = clickStreams.iterator();

                        if (!wikiSimIterator.hasNext()) {
                            return;
                        }

                        Tuple2<String, WikiSimComparableResultList<Double>> wikiSimRecord = wikiSimIterator.next();

                        HashMap<String, Integer> clickStream = new HashMap<>();
                        int maxClicks = 0;
                        while (clickStreamIterator.hasNext()) {
                            Tuple3<String, String, Integer> clickStreamRecord = clickStreamIterator.next();

                            String article = clickStreamRecord.getField(1);
                            int clicks = clickStreamRecord.getField(2);

                            clickStream.put(article, clicks);

                            if (clicks > maxClicks)
                                maxClicks = clicks;
                        }

                        // Skip if no ClickStream data is found
                        if (maxClicks == 0)
                            return;

                        // k = {10, 5, 1}
                        int[] k = new int[]{10, 5, 1};
                        double[] totalRelClicks = new double[]{0, 0, 0};
                        int[] totalClicks = new int[]{0, 0, 0};

                        List<WikiSimComparableResult<Double>> retrievedDocuments = Ordering.natural().greatestOf(
                                (WikiSimComparableResultList<Double>) wikiSimRecord.getField(1), k[0]);
                        ArrayList<Tuple4<String, Double, Integer, Double>> results = new ArrayList<>();

                        int rank = 1;
                        for (WikiSimComparableResult doc : retrievedDocuments) {
                            int clicks = clickStream.containsKey(doc.getName()) ? clickStream.get(doc.getName()) : 0;
                            double relClicks = maxClicks > 0 ? ((double) clicks) / ((double) maxClicks) : 0;

                            results.add(new Tuple4<>(
                                    doc.getName(),
                                    (Double) doc.getSortField1(),
                                    clicks,
                                    relClicks
                            ));

                            // loop k's
                            for (int i = 0; i < k.length; i++) {
                                if (rank <= k[i]) {
                                    totalClicks[i] += clicks;
                                    totalRelClicks[i] += relClicks;
                                }
                            }
                            rank++;
                        }

                        out.collect(new Tuple10<>(
                                (String) wikiSimRecord.getField(0),
                                results,
                                results.size(),
                                1,
                                totalClicks[0],
                                totalRelClicks[0],
                                totalClicks[1],
                                totalRelClicks[1],
                                totalClicks[2],
                                totalRelClicks[2]
                        ));

                    }
                });


        if (outputFilename.equals("print")) {
            output.print();
            // aggregate
            output.reduce(new ClickStreamAggregateOutput()).print();
        } else {
            output.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, String.valueOf(WikiSimConfiguration.csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
            output.reduce(new ClickStreamAggregateOutput()).writeAsCsv(outputAggregateFilename, WikiSimConfiguration.csvRowDelimiter, String.valueOf(WikiSimConfiguration.csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("ClickStreamEvaluation (Fields: Score=" + scoreField + "; Page=[" + fieldPageA + ";" + fieldPageB + "]");
    }

}
