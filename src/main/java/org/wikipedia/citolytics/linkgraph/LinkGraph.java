package org.wikipedia.citolytics.linkgraph;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.LinkTuple;
import org.wikipedia.citolytics.cpa.types.WikiDocument;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.redirects.single.WikiSimRedirects;
import org.wikipedia.processing.DocumentProcessor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.Math.abs;
import static java.lang.Math.max;

/**
 * Extracts detailed link graph of link pairs (LinkTuples) from Wikipedia.
 * <p/>
 * Input: List of LinkTuples
 * Output CSV: Article; LinkTuple; Distance
 */
public class LinkGraph extends WikiSimAbstractJob<Tuple4<String, String, String, Integer>> {

    public static void main(String[] args) throws Exception {
        new LinkGraph().enableSingleOutputFile().start(args);
    }

    public void plan() {

        if (args.length <= 3) {
            System.err.println("Input/output parameters missing!");
            System.err.println(getDescription());
            System.exit(1);
        }

        String inputWikiFilename = args[0];
        String inputLinkTuplesFilename = args[2];

        outputFilename = args[3];

        DataSet<Tuple2<String, String>> redirects = WikiSimRedirects.getRedirectsDataSet(env, args[1]);

        DataSet<Tuple2<String, String>> linkTupleList = env.readCsvFile(inputLinkTuplesFilename)
                .fieldDelimiter(WikiSimConfiguration.csvFieldDelimiter.charAt(0))
                .types(String.class, String.class)
                .coGroup(redirects)
                .where(1) // link B (Redirect target)
                .equalTo(1) // redirect target
                .with(new ReplaceLinkTuples(1))
                .coGroup(redirects)
                .where(0) // link A (Redirect target)
                .equalTo(1) // redirect target
                .with(new ReplaceLinkTuples(0));

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputWikiFilename);

        result = text.flatMap(new RichFlatMapFunction<String, Tuple4<String, String, String, Integer>>() {
            Collection<Tuple2<String, String>> linkTupleList;

            @Override
            public void open(Configuration parameters) throws Exception {
                linkTupleList = getRuntimeContext().getBroadcastVariable("linkTupleList");
            }

            @Override
            public void flatMap(String content, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
                LinkTuple linkTuple = new LinkTuple();

                WikiDocument doc = new DocumentProcessor().processDoc(content);
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
    }

    public static String getDescription() {
        return "Parameters: [WIKI DATASET] [REDIRECTS] [LINKTUPLE CSV] [OUTPUT]";
    }
}
