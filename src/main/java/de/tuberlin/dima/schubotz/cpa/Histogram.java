package de.tuberlin.dima.schubotz.cpa;

import de.tuberlin.dima.schubotz.cpa.contracts.HistogramMapper;
import de.tuberlin.dima.schubotz.cpa.contracts.HistogramReducer;
import de.tuberlin.dima.schubotz.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.cpa.types.HistogramResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * counts links & linkpairs
 */
public class Histogram {

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

        Configuration config = new Configuration();

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);


        // ArticleCounter, Links (, AvgDistance
        DataSet<HistogramResult> output = text.flatMap(new HistogramMapper())
                .groupBy(0)
                .reduceGroup(new HistogramReducer());

        output.writeAsText(outputFilename, FileSystem.WriteMode.OVERWRITE);

        env.execute("WikiHistogram");
    }


}
