package de.tuberlin.dima.schubotz.cpa;

import de.tuberlin.dima.schubotz.cpa.contracts.DocumentProcessor;
import de.tuberlin.dima.schubotz.cpa.contracts.calculateCPA;
import de.tuberlin.dima.schubotz.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.cpa.types.DataTypes.Result;
import de.tuberlin.dima.schubotz.cpa.types.DataTypes.ResultList;
import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by malteschwarzer on 08.11.14.
 */
public class WikiSim {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputFilename = ((args.length > 0) ? args[0] : "file:///Users/malteschwarzer/IdeaProjects/cpa-demo/target/test-classes/wikiParserTest1.xml");
        String outputFilename = ((args.length > 1) ? args[1] : "file:///Users/malteschwarzer/IdeaProjects/test.txt");

        String alpha = ((args.length > 2) ? args[2] : "1.5");
        String threshold = ((args.length > 3) ? args[3] : "1");

        Configuration config = new Configuration();

        config.setInteger("threshold", Integer.valueOf(threshold));
        config.setDouble("alpha", Double.valueOf(alpha));

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);


        DataSet<Result> res = text.flatMap(new DocumentProcessor())
                .groupBy(0) // Group by LinkTuple
                .reduceGroup(new calculateCPA())
                .withParameters(config);

        //res.writeAsText(outputFilename, FileSystem.WriteMode.OVERWRITE);
        res.writeAsCsv(outputFilename, "\n", ";\t", FileSystem.WriteMode.OVERWRITE);

        env.execute("WikiSim");
    }

    public String getDescription() {
        return "Parameters: [DATASET] [OUTPUT] [ALPHA] [THRESHOLD]";
    }
}
