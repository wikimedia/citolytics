package de.tuberlin.dima.schubotz.cpa.evaluation;

import de.tuberlin.dima.schubotz.cpa.WikiSim;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.ResultCoGrouper;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.SeeAlsoInputMapper;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.WikiSimGroupReducer;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.WikiSimInputMapper;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResultList;
import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;

public class BetterEvaluation {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println(new WikiSim().getDescription());
            System.exit(1);
        }

        String inputFilename = args[0];
        String outputFilename = args[1];
        String seeAlsoFilename = args[2];

        int cpaField = (args.length > 3 ? Integer.valueOf(args[3]) : 8);

        Configuration config = new Configuration();
        config.setInteger("fieldScore", cpaField);

        // See also
        DataSet<Tuple2<String, ArrayList<String>>> seeAlsoDataSet = env.readTextFile(seeAlsoFilename)
                .map(new SeeAlsoInputMapper());

        // WikiSim
        DataSet<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimDataSet = env.readTextFile(inputFilename)
                .flatMap(new WikiSimInputMapper())
                .withParameters(config)
                .groupBy(0)
                .reduceGroup(new WikiSimGroupReducer());

        // Evaluation
        DataSet<Tuple7<String, ArrayList<String>, Integer, WikiSimComparableResultList<Double>, Integer, Double, Double>> output = seeAlsoDataSet
                .coGroup(wikiSimDataSet)
                .where(0)
                .equalTo(0)
                .with(new ResultCoGrouper());

        if (outputFilename.equals("print")) {
            output.print();
        } else {
            output.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, String.valueOf(WikiSimConfiguration.csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("BetterEvaluation: CPA field: " + cpaField);
    }

}
