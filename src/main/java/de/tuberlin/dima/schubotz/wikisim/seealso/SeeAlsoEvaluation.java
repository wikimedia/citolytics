package de.tuberlin.dima.schubotz.wikisim.seealso;

import de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim;
import de.tuberlin.dima.schubotz.wikisim.cpa.utils.WikiSimConfiguration;
import de.tuberlin.dima.schubotz.wikisim.seealso.better.ResultCoGrouper;
import de.tuberlin.dima.schubotz.wikisim.seealso.better.SeeAlsoInputMapper;
import de.tuberlin.dima.schubotz.wikisim.seealso.better.WikiSimGroupReducer;
import de.tuberlin.dima.schubotz.wikisim.seealso.better.WikiSimInputMapper;
import de.tuberlin.dima.schubotz.wikisim.seealso.operators.BetterLinkExistsFilter;
import de.tuberlin.dima.schubotz.wikisim.seealso.operators.BetterSeeAlsoLinkExistsFilter;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResultList;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

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
 * 4 = SCORE-FIELD: column of score field in result set (default: 8)
 * 5 = PAGE-A-FIELD: column of page A in result set (default: 1)
 * 6 = PAGE-B-FIELD: column of page B in result set (default: 2)
 */
public class SeeAlsoEvaluation {
    public static String outputFilename;
    public static String seeAlsoInputFilename;
    public static String wikiSimInputFilename;
    public static String linksInputFilename;

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
        outputFilename = args[1];
        seeAlsoInputFilename = args[2];
        linksInputFilename = args[3];

        int scoreField = (args.length > 4 ? Integer.valueOf(args[4]) : 8);
        int fieldPageA = (args.length > 5 ? Integer.valueOf(args[5]) : 1);
        int fieldPageB = (args.length > 6 ? Integer.valueOf(args[6]) : 2);


        Configuration config = new Configuration();

        config.setInteger("fieldPageA", fieldPageA);
        config.setInteger("fieldPageB", fieldPageB);
        config.setInteger("fieldScore", scoreField);

        // See also
        DataSet<Tuple2<String, ArrayList<String>>> seeAlsoDataSet = env.readTextFile(seeAlsoInputFilename)
                .map(new SeeAlsoInputMapper());

        // WikiSim
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

        DataSet<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimGroupedDataSet = wikiSimDataSet
                .groupBy(0)
                .reduceGroup(new WikiSimGroupReducer());

        // Evaluation
        DataSet<Tuple11<String, ArrayList<String>, Integer, WikiSimComparableResultList<Double>, Integer, Double, Double, Double, Integer, Integer, Integer>> output = seeAlsoDataSet
                .coGroup(wikiSimGroupedDataSet)
                .where(0)
                .equalTo(0)
                .with(new ResultCoGrouper());

        if (outputFilename.equals("print")) {
            output.print();
        } else {
            output.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, String.valueOf(WikiSimConfiguration.csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("BetterEvaluation (Fields: Score=" + scoreField + "; Page=[" + fieldPageA + ";" + fieldPageB + "]");
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
