package de.tuberlin.dima.schubotz.wikisim.seealso;

import de.tuberlin.dima.schubotz.wikisim.WikiSimJob;
import de.tuberlin.dima.schubotz.wikisim.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.wikisim.cpa.operators.DocumentProcessor;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiDocument;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Extracts "See also" links from Wikipedia articles and creates CSV for DB import
 * <p/>
 * TODO: Check if link exists in article body
 * <p/>
 * table structure: article (primary key), "see also"-link target, position, total count of "see also"-links,
 */
public class SeeAlsoExtractor extends WikiSimJob<Tuple4<String, String, Integer, Integer>> {


    public static void main(String[] args) throws Exception {
        new SeeAlsoExtractor().start(args);
    }

    public void plan() {

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println("Usage: [INPUT] [OUTPUT]");
            System.exit(1);
        }

        String inputFilename = args[0];
        outputFilename = args[1];

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        // ArticleCounter, Links (, AvgDistance
        result = text.flatMap(new FlatMapFunction<String, Tuple4<String, String, Integer, Integer>>() {
            public void flatMap(String content, Collector out) {

                WikiDocument doc = new DocumentProcessor().processDoc(content, true);

                if (doc == null) return;

                List<Map.Entry<String, Integer>> links = doc.getOutLinks();

                int pos = 1;
                for (Map.Entry<String, Integer> outLink : links) {

                    out.collect(new Tuple4<>(doc.getTitle(), outLink.getKey(), pos, links.size()));
                    pos++;
                }
            }
        });

    }

}
