package de.tuberlin.dima.schubotz.cpa;

import de.tuberlin.dima.schubotz.cpa.contracts.DocumentProcessor;
import de.tuberlin.dima.schubotz.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.cpa.types.LinkTuple;
import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.Math.abs;
import static java.lang.Math.max;

/**
 * Input: List of LinkTuples
 * Output CSV: Article; LinkTuple; Distance
 */
public class LinkGraph {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length <= 2) {
            System.err.println("Input/output parameters missing!");
            System.err.println(getDescription());
            System.exit(1);
        }

        String inputWikiFilename = args[0];
        String inputLinkTuplesFilename = args[1];

        String outputFilename = args[2];

        DataSet<Tuple2<String, String>> linkTupleList = env.readCsvFile(inputLinkTuplesFilename)
                .fieldDelimiter(WikiSimConfiguration.csvFieldDelimiter.charAt(0))
                .types(String.class, String.class);

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputWikiFilename);

        DataSet<Tuple4<String, String, String, Integer>> result = text.flatMap(new RichFlatMapFunction<String, Tuple4<String, String, String, Integer>>() {
            Collection<Tuple2<String, String>> linkTupleList;

            @Override
            public void open(Configuration parameters) throws Exception {
                linkTupleList = getRuntimeContext().getBroadcastVariable("linkTupleList");
            }

            @Override
            public void flatMap(String content, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
                LinkTuple linkTuple = new LinkTuple();

                WikiDocument doc = DocumentProcessor.processDoc(content);
                if (doc == null) return;

                // Get links & wordmap
                List<Map.Entry<String, Integer>> outLinks = doc.getOutLinks();
                TreeMap<Integer, Integer> wordMap = doc.getWordMap();

                // Loop all link pairs
                for (Map.Entry<String, Integer> outLink1 : outLinks) {
                    for (Map.Entry<String, Integer> outLink2 : outLinks) {
                        int order = outLink1.getKey().compareTo(outLink2.getKey());
                        if (order > 0) {
                            int w1 = wordMap.floorEntry(outLink1.getValue()).getValue();
                            int w2 = wordMap.floorEntry(outLink2.getValue()).getValue();
                            int d = max(abs(w1 - w2), 1);
                            //recDistance.setValue(1 / (pow(d, α)));

                            linkTuple.setFirst(outLink1.getKey());
                            linkTuple.setSecond(outLink2.getKey());

                            // Add result to collector
                            if (linkTuple.isValid() && (linkTupleList.contains(linkTuple) || linkTupleList.contains(linkTuple.getTwin()))) {
                                out.collect(new Tuple4<>(
                                                doc.getTitle(),
                                                linkTuple.getFirst(),
                                                linkTuple.getSecond(),
                                                d)
                                );
                            }
                        }
                    }
                }

            }
        }).withBroadcastSet(linkTupleList, "linkTupleList");


        result.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, WikiSimConfiguration.csvFieldDelimiter, FileSystem.WriteMode.OVERWRITE);

        env.execute("LinkGraph");

    }

    public static String getDescription() {
        return "Parameters: [WIKI DATASET] [LINKTUPLE CSV] [OUTPUT]";
    }
}
