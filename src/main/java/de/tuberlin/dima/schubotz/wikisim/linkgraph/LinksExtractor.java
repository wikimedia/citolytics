package de.tuberlin.dima.schubotz.wikisim.linkgraph;

import de.tuberlin.dima.schubotz.wikisim.WikiSimJob;
import de.tuberlin.dima.schubotz.wikisim.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.wikisim.cpa.operators.DocumentProcessor;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiDocument;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Extracts internal links from Wikipedia articles and creates CSV for DB import
 * <p/>
 * table structure: article (primary key), link target
 */
public class LinksExtractor extends WikiSimJob<Tuple2<String, String>> {

    public static void main(String[] args) throws Exception {
        new LinksExtractor().start(args);
    }

    public void plan() {

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println("USAGE: <wiki-xml-dump> <output>");
            System.exit(1);
        }

        String inputFilename = args[0];
        outputFilename = args[1];

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        // ArticleCounter, Links (, AvgDistance
        result = text.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            public void flatMap(String content, Collector out) {

                WikiDocument doc = new DocumentProcessor().processDoc(content);
                if (doc == null) return;

                List<Map.Entry<String, Integer>> links = doc.getOutLinks();

                for (Map.Entry<String, Integer> outLink : links) {

                    out.collect(new Tuple2<>(
                            doc.getTitle(),
                            outLink.getKey()
                    ));
                    //System.out.println(outLink.getKey());
                }
            }
        }).distinct();
    }
}
