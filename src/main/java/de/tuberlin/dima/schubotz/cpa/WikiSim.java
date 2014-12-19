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

import de.tuberlin.dima.schubotz.cpa.contracts.calculateCPA;
import de.tuberlin.dima.schubotz.cpa.contracts.DocumentProcessor;
import de.tuberlin.dima.schubotz.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.cpa.types.WikiSimResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * Run with flink run -c de.tuberlin.dima.schubotz.cpa.WikiSim INPUTFILE OUTPUTFILE [alpha] [reducerThreshold] [combinerThreshold]
 */
public class WikiSim {

    public static String csvRowDelimiter = "\n";
    public static String csvFieldDelimiter = "|";

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

        String alpha = ((args.length > 2) ? args[2] : "1.5");
        String reducerThreshold = ((args.length > 3) ? args[3] : "1");
        String combinerThreshold = ((args.length > 4) ? args[4] : "1");

        Configuration config = new Configuration();

        config.setInteger("reducerThreshold", Integer.valueOf(reducerThreshold));
        config.setInteger("combinerThreshold", Integer.valueOf(combinerThreshold));
        config.setDouble("alpha", Double.valueOf(alpha));
        config.setBoolean("median", true);

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);


        DataSet<WikiSimResult> res = text.flatMap(new DocumentProcessor())
                .groupBy(0) // Group by LinkTuple
                .reduceGroup(new calculateCPA())

                .withParameters(config)

                // reduce -> Median
                ;

        //res.writeAsText(outputFilename, FileSystem.WriteMode.OVERWRITE);
        res.writeAsCsv(outputFilename, csvRowDelimiter, csvFieldDelimiter, FileSystem.WriteMode.OVERWRITE);

        env.execute("WikiSim");
    }

    public String getDescription() {
        return "Parameters: [DATASET] [OUTPUT] [ALPHA] [REDUCER-THRESHOLD] [COMBINER-THRESHOLD]";
    }
}
