package org.wikipedia.citolytics.seealso;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.wikipedia.citolytics.WikiSimJob;
import org.wikipedia.citolytics.seealso.better.*;
import org.wikipedia.citolytics.seealso.operators.BetterLinkExistsFilter;
import org.wikipedia.citolytics.seealso.operators.BetterSeeAlsoLinkExistsFilter;
import org.wikipedia.citolytics.seealso.types.SeeAlsoEvaluationResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Pattern;

/**
 * Flink job for running a "See also"-based evaluation on CPA or MLT result sets.
 * <p/>
 * Arguments:
 * 0 = RESULT-SET: path to results of CPA/CoCit or MLT
 * 1 = OUTPUT: filename of results (HDFS or print)
 * 2 = SEEALSO: path to extracted "See also"-links (output of wikisim.seealso.SeeAlsoExtractor)
 * 3 = LINKS-SET: path to extracted wiki links for filtering existing links (output of wikisim.linkgraph.LinksExtractor)
 * 4 = SCORE-FIELD: column of score field in result set (default: 6)
 * 5 = PAGE-A-FIELD: column of page A in result set (default: 1)
 * 6 = PAGE-B-FIELD: column of page B in result set (default: 2)
 * 7 = ENABLE-MRR: if set, performance measure is MRR (default: MAP)
 * <p/>
 * Set SCORE-FIELD = -1 for MLT result data set.
 */
public class SeeAlsoEvaluation extends WikiSimJob<SeeAlsoEvaluationResult> {
    public static String seeAlsoInputFilename;
    public static String wikiSimInputFilename;
    public static String linksInputFilename;

    public final int topK = 10;

    public static DataSet<Tuple2<String, HashSet<String>>> links;

    public static void main(String[] args) throws Exception {
        new SeeAlsoEvaluation().start(args);
    }

    public void plan() {
        if (args.length < 3) {
            System.err.println("Input/output parameters missing!");
            System.err.println("USAGE: <result-set> <output> <seealso-set> [<links (=nofilter)>] [<score-field>] [<page-a-field>] [<page-b-field>] [enable-mrr]");
            System.exit(1);
        }
        setJobName("SeeAlso Evaluation");

        wikiSimInputFilename = args[0];
        outputFilename = args[1];
        seeAlsoInputFilename = args[2];
        linksInputFilename = (args.length > 3 ? args[3] : "nofilter");

        int scoreField = (args.length > 4 ? Integer.valueOf(args[4]) : 5);
        int fieldPageA = (args.length > 5 ? Integer.valueOf(args[5]) : 1);
        int fieldPageB = (args.length > 6 ? Integer.valueOf(args[6]) : 2);
        boolean enableMRR = (args.length > 7 && args[7] != "" ? true : false);

        // See also
        DataSet<Tuple2<String, ArrayList<String>>> seeAlsoDataSet = env.readTextFile(seeAlsoInputFilename)
                .map(new SeeAlsoInputMapper());

        // Read result set
        DataSet<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimGroupedDataSet;

        // CPA or MLT results?
        if (scoreField >= 0 && fieldPageA >= 0 && fieldPageB >= 0) {
            // CPA
            jobName += " CPA score = " + scoreField + "; pages = " + fieldPageA + "; " + fieldPageB;
            Configuration config = new Configuration();

            config.setInteger("fieldPageA", fieldPageA);
            config.setInteger("fieldPageB", fieldPageB);
            config.setInteger("fieldScore", scoreField);

            DataSet<Tuple3<String, String, Double>> wikiSimDataSet = env.readTextFile(wikiSimInputFilename)
                    .flatMap(new WikiSimInputMapper())
                    .withParameters(config);

            // LinkFilter
            if (!linksInputFilename.isEmpty() && !linksInputFilename.equals("nofilter")) {
                wikiSimDataSet = wikiSimDataSet
                        .coGroup(getLinkDataSet(env, linksInputFilename))
                        .where(0)
                        .equalTo(0)
                        .with(new BetterLinkExistsFilter());

                seeAlsoDataSet = seeAlsoDataSet
                        .coGroup(getLinkDataSet(env, linksInputFilename))
                        .where(0)
                        .equalTo(0)
                        .with(new BetterSeeAlsoLinkExistsFilter());
            }

            wikiSimGroupedDataSet = wikiSimDataSet
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
        result = seeAlsoDataSet
                .coGroup(wikiSimGroupedDataSet)
                .where(0)
                .equalTo(0)
                .with(new EvaluateSeeAlso(topK, enableMRR));
    }


    public static DataSet<Tuple2<String, HashSet<String>>> getLinkDataSet(ExecutionEnvironment env, String filename) {
        if (links == null) {
            links = env.readTextFile(filename)

                    .map(new MapFunction<String, Tuple2<String, HashSet<String>>>() {
                        Pattern delimiter = Pattern.compile(Pattern.quote("|"));

                        @Override
                        public Tuple2<String, HashSet<String>> map(String in) throws Exception {
                            String[] cols = delimiter.split(in);
                            return new Tuple2<>(
                                    cols[0],
                                    new HashSet<>(Arrays.asList(cols[1]))
                            );
                        }
                    })
                    .groupBy(0)
                    .reduce(new ReduceFunction<Tuple2<String, HashSet<String>>>() {
                        @Override
                        public Tuple2<String, HashSet<String>> reduce(Tuple2<String, HashSet<String>> a, Tuple2<String, HashSet<String>> b) throws Exception {
                            HashSet<String> set = (HashSet) a.getField(1);
                            set.addAll((HashSet) b.getField(1));
                            return new Tuple2<>(
                                    (String) a.getField(0),
                                    set);
                        }
                    });
        }

        return links;
    }
}
