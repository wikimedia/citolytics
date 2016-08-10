package org.wikipedia.citolytics.stats;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.linkgraph.LinksExtractor;
import org.wikipedia.citolytics.redirects.single.WikiSimRedirects;

import java.util.Iterator;


/**
 * Combine output of ArticleStats and LinksExtractor to add inbound link stats.
 */
public class ArticleStatsWithInboundLinks extends WikiSimJob<Tuple6<String, Integer, Integer, Integer, Double, Integer>> {
    public static void main(String[] args) throws Exception {
        new ArticleStatsWithInboundLinks().start(args);
    }

    public void plan() throws Exception {

        if (args.length < 2) {
            System.err.println("Input/output parameters missing!");
            System.err.println("Arguments: <WIKI-XML> <OUTPUT-FILE> [REDIRECTS-FILE]");
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

        // Resolve redirects in inbound links
        if (args.length > 2) {
            DataSet<Tuple2<String, String>> redirects = WikiSimRedirects.getRedirectsDataSet(env, args[2]);

            links = links
                    .coGroup(redirects)
                    .where(0)
                    .equalTo(0)
                            // Replace redirects
                    .with(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, String>, Tuple2<String, Integer>>() {
                        @Override
                        public void coGroup(Iterable<Tuple2<String, Integer>> links, Iterable<Tuple2<String, String>> redirects, Collector<Tuple2<String, Integer>> out) throws Exception {
                            Iterator<Tuple2<String, Integer>> iteratorLinks = links.iterator();
                            Iterator<Tuple2<String, String>> iteratorRedirects = redirects.iterator();

                            if (iteratorLinks.hasNext()) {
                                Tuple2<String, Integer> link = iteratorLinks.next();

                                if (iteratorRedirects.hasNext()) {
                                    Tuple2<String, String> redirect = iteratorRedirects.next();

                                    link.f0 = redirect.f1;
                                }

                                out.collect(link);
                            }
                        }
                    })
                            // Merge duplicates
                    .groupBy(0)
                    .aggregate(Aggregations.SUM, 1)
            ;
        }

        result = stats
                .coGroup(links)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<ArticleTuple, Tuple2<String, Integer>, Tuple6<String, Integer, Integer, Integer, Double, Integer>>() {
                    @Override
                    public void coGroup(Iterable<ArticleTuple> statsIn, Iterable<Tuple2<String, Integer>> linksIn, Collector<Tuple6<String, Integer, Integer, Integer, Double, Integer>> out) throws Exception {
                        Iterator<ArticleTuple> iterator = statsIn.iterator();
                        Iterator<Tuple2<String, Integer>> linksIterator = linksIn.iterator();

                        if (iterator.hasNext()) {
                            ArticleTuple stats = iterator.next();
                            int inboundLinks = 0;

                            if (linksIterator.hasNext()) {
                                Tuple2<String, Integer> links = linksIterator.next();

                                inboundLinks = links.f1;
                            }

                            out.collect(new Tuple6<>(
                                    stats.f0,
                                    stats.f1,
                                    stats.f2,
                                    stats.f3,
                                    stats.f4,
                                    inboundLinks
                            ));
                        }
                    }
                });

    }
}
