package org.wikipedia.citolytics.stats;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.WikiDocument;
import org.wikipedia.processing.DocumentProcessor;

/**
 * Collect following values for each article in Wikipedia XML Dump:
 *
 * words, headlines, outLinks, avgLinkDistance
 */
public class ArticleStats extends WikiSimAbstractJob<ArticleTuple> {

    public static void main(String[] args) throws Exception {
        new ArticleStats().start(args);
    }

    public void plan() {

        if (args.length < 2) {
            System.err.println("Input/output parameters missing!");
            System.err.println("Arguments: <WIKISET> <OUTPUT-FILE>");
            System.exit(1);
        }

        String inputWikiSet = args[0];
        outputFilename = args[1];

        // Collect stats from articles
        result = env.readFile(new WikiDocumentDelimitedInputFormat(), inputWikiSet)
                .flatMap(new FlatMapFunction<String, ArticleTuple>() {
                    public void flatMap(String content, Collector out) {
                        collectStats(content, out);
                    }
                });

    }

    public static void collectStats(String content, Collector<ArticleTuple> out) {
        WikiDocument doc = new DocumentProcessor().processDoc(content);
        if (doc == null) return;

        int words = doc.getWordMap().size();
        int outLinks = doc.getOutLinks().size();
        int headlines = doc.getHeadlines().size();
        double avgLinkDistance = doc.getAvgLinkDistance();

        out.collect(new ArticleTuple(
                        doc.getTitle(),
                        words,
                        headlines,
                        outLinks,
                        avgLinkDistance
                )

        );
    }
}
