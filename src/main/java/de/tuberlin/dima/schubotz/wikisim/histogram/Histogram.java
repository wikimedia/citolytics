package de.tuberlin.dima.schubotz.wikisim.histogram;

import de.tuberlin.dima.schubotz.wikisim.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.wikisim.cpa.utils.WikiSimOutputWriter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * Generate statistic from Wikipedia XML Dump. Counts links & link pairs
 */
public class Histogram {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println("USAGE: <input> <output>");
            System.exit(1);
        }

        String inputFilename = args[0];
        String outputFilename = args[1];

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        // ArticleCounter, Links (, AvgDistance
        DataSet<HistogramResult> output = text.flatMap(new HistogramMapper())
                .reduce(new HistogramReducer());

        new WikiSimOutputWriter<HistogramResult>("WikiHistogram (ns, articlecount, linkcount, linkpairs)")
                .asText()
                .write(env, output, outputFilename);

    }
}
