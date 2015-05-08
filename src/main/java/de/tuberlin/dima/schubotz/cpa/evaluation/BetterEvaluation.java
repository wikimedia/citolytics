package de.tuberlin.dima.schubotz.cpa.evaluation;

import de.tuberlin.dima.schubotz.cpa.WikiSim;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.ResultCoGrouper;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.SeeAlsoInputMapper;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.WikiSimGroupReducer;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.WikiSimInputMapper;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.BetterLinkExistsFilter;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.BetterSeeAlsoLinkExistsFilter;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResultList;
import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
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

public class BetterEvaluation {
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

        System.out.println(seeAlsoInputFilename);

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
        if (linksInputFilename.isEmpty() || linksInputFilename.equals("nofilter")) {
            wikiSimDataSet = wikiSimDataSet
                    .coGroup(getLinkDataSet(env))
                    .where(0)
                    .equalTo(0)
                    .with(new BetterLinkExistsFilter());

            seeAlsoDataSet = seeAlsoDataSet
                    .coGroup(getLinkDataSet(env))
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


    public static DataSet<Tuple2<String, HashSet<String>>> getLinkDataSet(ExecutionEnvironment env) {
        if (links == null) {
            links = env.readTextFile(linksInputFilename)

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

//        links.print();
        }

        return links;
//        return env.fromElements(
//                new Tuple2<>("QQQ", new HashSet<>(Arrays.asList(new String[]{"MLT link"})))
//        );
    }
}
