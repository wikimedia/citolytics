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
import de.tuberlin.dima.schubotz.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * Run with flink run -c de.tuberlin.dima.schubotz.cpa.WikiSim INPUTFILE OUTPUTFILE [alpha] [reducerThreshold] [combinerThreshold]
 */
public class WikiSim {

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
        config.setString("alpha", alpha);
        config.setBoolean("median", true);
        config.setBoolean("wiki2006", true);

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        DataSet<WikiSimResult> output = text.flatMap(new DocumentProcessor())
                .withParameters(config)
                .groupBy(0) // Group by LinkTuple
                .reduceGroup(new calculateCPA())

                .withParameters(config)

                // reduce -> Median
                ;

        if (outputFilename.equals("print")) {
            output.print();
        } else {
            output.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, WikiSimConfiguration.csvFieldDelimiter, FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("WikiSim");
    }

    public String getDescription() {
        return "Parameters: [DATASET] [OUTPUT] [ALPHA1, ALPHA2, ...] [REDUCER-THRESHOLD] [COMBINER-THRESHOLD]";
    }
}
