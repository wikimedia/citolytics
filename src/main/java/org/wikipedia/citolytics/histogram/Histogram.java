package org.wikipedia.citolytics.histogram;

import org.apache.flink.api.java.operators.DataSource;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;

/**
 * Generate statistic from Wikipedia XML Dump. Counts links & link pairs
 */
public class Histogram extends WikiSimAbstractJob<HistogramResult> {

    public static void main(String[] args) throws Exception {
        new Histogram()
                .setJobName("WikiHistogram (ns, articlecount, linkcount, linkpairs)")
                .enableTextOutput()
                .start(args);
    }

    public void plan() {
        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println("USAGE: <input> <output>");
            System.exit(1);
        }

        String inputFilename = args[0];
        outputFilename = args[1];

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        // ArticleCounter, Links (, AvgDistance
        result = text.flatMap(new HistogramMapper())
                .reduce(new HistogramReducer());

    }
}
