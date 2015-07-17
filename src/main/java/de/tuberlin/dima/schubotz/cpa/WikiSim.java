/* __
* \ \
* _ _ \ \ ______
* | | | | > \( __ )
* | |_| |/ ^ \| || |
* | ._,_/_/ \_\_||_|
* | |
* |_|
*
* ----------------------------------------------------------------------------
* "THE BEER-WARE LICENSE" (Revision 42):
* <rob ∂ CLABS dot CC> wrote this file. As long as you retain this notice you
* can do whatever you want with this stuff. If we meet some day, and you think
* this stuff is worth it, you can buy me a beer in return.
* ----------------------------------------------------------------------------
*/
package de.tuberlin.dima.schubotz.cpa;

import de.tuberlin.dima.schubotz.cpa.contracts.DocumentProcessor;
import de.tuberlin.dima.schubotz.cpa.contracts.calculateCPA;
import de.tuberlin.dima.schubotz.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.cpa.redirects.ReduceResults;
import de.tuberlin.dima.schubotz.cpa.redirects.ReplaceRedirects;
import de.tuberlin.dima.schubotz.cpa.redirects.WikiSimRedirects;
import de.tuberlin.dima.schubotz.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;

/**
 * Run with flink run -c de.tuberlin.dima.schubotz.cpa.WikiSim INPUTFILE OUTPUTFILE [alpha] [reducerThreshold] [combinerThreshold] [format] [redirects-file]
 */
public class WikiSim {
    public static String jobName = "WikiSim";

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // Bug fix (heartbeat bug)
        // https://issues.apache.org/jira/browse/FLINK-2299

        Configuration clusterConfig = new Configuration();
        clusterConfig.setString(ConfigConstants.AKKA_TRANSPORT_HEARTBEAT_PAUSE, "600s");
        GlobalConfiguration.includeConfiguration(clusterConfig);

        // ---

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println(new WikiSim().getDescription());
            System.exit(1);
        }

        String inputFilename = args[0];
        String outputFilename = args[1];

        String alpha = ((args.length > 2) ? args[2] : "1.5");
        String reducerThreshold = ((args.length > 3) ? args[3] : "1");
        String combinerThreshold = ((args.length > 4) ? args[4] : "1");
        boolean redirected = ((args.length > 6 && !args[6].equalsIgnoreCase("n")) ? true : false);

        Configuration config = new Configuration();

        config.setInteger("reducerThreshold", Integer.valueOf(reducerThreshold));
        config.setInteger("combinerThreshold", Integer.valueOf(combinerThreshold));
        config.setString("alpha", alpha);
        config.setBoolean("median", true);
        config.setBoolean("wiki2006", (args.length > 5 && args[5].equals("2006") ? true : false));

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        // Calculate results
        DataSet<WikiSimResult> wikiSimResults = text.flatMap(new DocumentProcessor())
                .withParameters(config)
                .groupBy(0) // Group by LinkTuple
                .reduceGroup(new calculateCPA())

                .withParameters(config)

                // reduce -> Median
                ;

        // Merge redirects
        if (redirected) {
            jobName += " with redirects";

            // fields
            int hash = 0;
            int pageA = 1;
            int pageB = 2;
            int redirectSource = 0;
            int redirectTarget = 1;

            DataSet<Tuple2<String, String>> redirects = WikiSimRedirects.getRedirectsDataSet(env, args[6]);

            DataSet<WikiSimResult> redirectedResults = wikiSimResults

                    // replace page names with redirect target
                    // page A
                    .coGroup(redirects)
                    .where(pageA)
                    .equalTo(redirectSource)
                    .with(new ReplaceRedirects(pageA))
                            // page B
                    .coGroup(redirects)
                    .where(pageB)
                    .equalTo(redirectSource)
                    .with(new ReplaceRedirects(pageB))
                            // sum duplicated tuples
                    .groupBy(hash)
                    .reduceGroup(new ReduceResults());

            writeOutput(env, redirectedResults, outputFilename, jobName);
        } else {
            // Write undirected output
            writeOutput(env, wikiSimResults, outputFilename, jobName);
        }
    }

    public static void writeOutput(ExecutionEnvironment env, DataSet dataSet, String outputFilename, String jobName) throws Exception {
        if (outputFilename.equals("print")) {
            dataSet.print();
        } else {
            dataSet.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, WikiSimConfiguration.csvFieldDelimiter, FileSystem.WriteMode.OVERWRITE);
            env.execute(jobName);
        }
    }

    public String getDescription() {
        return "Parameters: [DATASET] [OUTPUT] [ALPHA1, ALPHA2, ...] [REDUCER-THRESHOLD] [COMBINER-THRESHOLD] [WIKI-VERSION]";
    }
}
