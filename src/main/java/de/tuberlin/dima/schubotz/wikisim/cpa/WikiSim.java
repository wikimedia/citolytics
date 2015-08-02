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
package de.tuberlin.dima.schubotz.wikisim.cpa;

import de.tuberlin.dima.schubotz.wikisim.WikiSimJob;
import de.tuberlin.dima.schubotz.wikisim.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.wikisim.cpa.operators.CPAReducer;
import de.tuberlin.dima.schubotz.wikisim.cpa.operators.DocumentProcessor;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.wikisim.redirects.ReduceResults;
import de.tuberlin.dima.schubotz.wikisim.redirects.ReplaceRedirects;
import de.tuberlin.dima.schubotz.wikisim.redirects.single.WikiSimRedirects;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;

/**
 * Flink job for computing CPA results depended on CPI alpha values.
 * <p/>
 * Arguments:
 * 0 = Wikipedia XML Dump (hdfs)
 * 1 = Output filename (hdfs)
 * 2 = CPI alpha values, each value is represented by a separate column in the output (comma separated; e.g. 0.5,1.0,1.5)
 * 3 = Reducer threshold: Discard records with lower number of co-citations (default: 1)
 * 4 = Combiner threshold: Discard records with lower number of co-citations (default: 1)
 * 5 = Format of Wikipedia XML Dump (default: 2013; set to "2006" for older dumps)
 * 6 = Resolve redirects? Set path to redirects set (default: n)
 */
public class WikiSim extends WikiSimJob<WikiSimResult> {

    public static String getUsage() {
        return "Usage: [DATASET] [OUTPUT] [ALPHA1, ALPHA2, ...] [REDUCER-THRESHOLD] [COMBINER-THRESHOLD] [WIKI-VERSION] [REDIRECTS-DATESET]";
    }

    /**
     * Executes Flink job
     *
     * @param args {input, output, alpha, reducerThreshold, combinerThreshold, wiki-format, redirects}
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new WikiSim().start(args);
    }

    public void plan() {
        jobName = "WikiSim";

        // Bug fix (heartbeat bug)
        // https://issues.apache.org/jira/browse/FLINK-2299

        Configuration clusterConfig = new Configuration();
        clusterConfig.setString(ConfigConstants.AKKA_TRANSPORT_HEARTBEAT_PAUSE, "600s");
        GlobalConfiguration.includeConfiguration(clusterConfig);

        // ---

        if (args.length <= 1) {
            System.err.println("Error: Input/output parameters missing!");
            System.err.println(WikiSim.getUsage());
            System.exit(1);
        }

        // Read arguments
        String inputFilename = args[0];
        outputFilename = args[1];

        boolean redirected = ((args.length > 6 && !args[6].equalsIgnoreCase("n")) ? true : false);

        // Configuration for 2nd order functions
        Configuration config = new Configuration();
        config.setString("alpha", ((args.length > 2) ? args[2] : "1.5"));
        config.setInteger("reducerThreshold", (args.length > 3) ? Integer.valueOf(args[3]) : 1);
        config.setInteger("combinerThreshold", (args.length > 4) ? Integer.valueOf(args[4]) : 1);

        config.setBoolean("median", true);
        config.setBoolean("wiki2006", (args.length > 5 && args[5].equals("2006") ? true : false));

        // Read Wikipedia XML Dump
        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        // Calculate results
        result = text.flatMap(new DocumentProcessor())
                .withParameters(config)
                .groupBy(0) // Group by LinkTuple.hash()
                .reduceGroup(new CPAReducer())
                .withParameters(config);


        // Resolve redirects if requested
        if (redirected) {
            jobName += " with redirects";
            result = resolveRedirects(env, result, args[6]);
        }
    }

//    /**
//     * Write output to CSV file or print to console.
//     *
//     * @param env
//     * @param dataSet
//     * @param outputFilename
//     * @param jobName
//     * @throws Exception
//     */
//    public static void writeOutput(ExecutionEnvironment env, DataSet dataSet, String outputFilename, String jobName) throws Exception {
//        if (outputFilename.equals("print")) {
//            dataSet.print();
//        } else {
//            //dataSet.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, WikiSimConfiguration.csvFieldDelimiter, FileSystem.WriteMode.OVERWRITE);
//            dataSet.write(new WikiOutputFormat<WikiSimResult>(outputFilename), outputFilename, FileSystem.WriteMode.OVERWRITE);
//            env.execute(jobName);
//        }
//    }

    /**
     * Resolve Wikipedia redirects in results
     * <p/>
     * Wikipedia uses inconsistent internal links, therefore, we need to check each result record
     * for redirects, map redirects to their targets and sum up resulting duplicates.
     *
     * @param env
     * @param wikiSimResults
     * @param filename
     * @return Result set with resolved redirects
     */
    public static DataSet<WikiSimResult> resolveRedirects(ExecutionEnvironment env, DataSet<WikiSimResult> wikiSimResults, String filename) {
        // fields
        int hash = 0;
        int pageA = 1;
        int pageB = 2;
        int redirectSource = 0;
        int redirectTarget = 1;

        DataSet<Tuple2<String, String>> redirects = WikiSimRedirects.getRedirectsDataSet(env, filename);

        return wikiSimResults
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
    }


}
