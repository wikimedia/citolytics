package de.tuberlin.dima.schubotz.wikisim.stats;

import de.tuberlin.dima.schubotz.wikisim.WikiSimJob;
import de.tuberlin.dima.schubotz.wikisim.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.wikisim.linkgraph.LinksExtractor;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

import java.util.Iterator;


/**
 * Combine output of ArticleStats and LinksExtractor to add inbound link stats.
 */
public class ArticleStatsWithInboundLinks extends WikiSimJob<Tuple7<String, Integer, Integer, Integer, Double, Double, Integer>> {
    public static void main(String[] args) throws Exception {
        new ArticleStatsWithInboundLinks().start(args);
    }

    public void plan() throws Exception {

        if (args.length < 2) {
            System.err.println("Input/output parameters missing!");
            System.err.println("Arguments: <WIKI-XML> <OUTPUT-FILE>");
            System.exit(1);
        }

        outputFilename = args[1];

        DataSet<ArticleTuple> stats = env.readFile(new WikiDocumentDelimitedInputFormat(), args[0])
                .flatMap(new FlatMapFunction<String, ArticleTuple>() {
                    public void flatMap(String content, Collector out) {
                        ArticleStats.collectStats(content, out);
                    }
                });

        DataSet<Tuple2<String, Integer>> links =
                env.readFile(new WikiDocumentDelimitedInputFormat(), args[0])
                        .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                            public void flatMap(String content, Collector out) {
                                LinksExtractor.collectLinks(content, out);
                            }
                        })
                        .distinct()
                        .groupBy(1) // group by link-target
                        .reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, Integer>>() {
                            @Override
                            public void reduce(Iterable<Tuple2<String, String>> in, Collector<Tuple2<String, Integer>> out) throws Exception {
                                Iterator<Tuple2<String, String>> iterator = in.iterator();
                                int inboundLinks = 0;
                                Tuple2<String, String> current = null;

                                // count inbound links
                                while (iterator.hasNext()) {
                                    current = iterator.next();
                                    inboundLinks++;
                                }

                                out.collect(new Tuple2<>(current.f1, inboundLinks));
                            }
                        });

        result = stats
                .coGroup(links)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<ArticleTuple, Tuple2<String, Integer>, Tuple7<String, Integer, Integer, Integer, Double, Double, Integer>>() {
                    @Override
                    public void coGroup(Iterable<ArticleTuple> statsIn, Iterable<Tuple2<String, Integer>> linksIn, Collector<Tuple7<String, Integer, Integer, Integer, Double, Double, Integer>> out) throws Exception {
                        Iterator<ArticleTuple> iterator = statsIn.iterator();
                        Iterator<Tuple2<String, Integer>> linksIterator = linksIn.iterator();

                        if (iterator.hasNext()) {
                            ArticleTuple stats = iterator.next();
                            int inboundLinks = 0;

                            if (linksIterator.hasNext()) {
                                Tuple2<String, Integer> links = linksIterator.next();

                                inboundLinks = links.f1;
                            }

                            out.collect(new Tuple7<>(
                                    stats.f0,
                                    stats.f1,
                                    stats.f2,
                                    stats.f3,
                                    stats.f4,
                                    stats.f5,
                                    inboundLinks
                            ));
                        }
                    }
                });

    }
}
