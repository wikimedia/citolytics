package de.tuberlin.dima.schubotz.wikisim.stats;

import de.tuberlin.dima.schubotz.wikisim.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.wikisim.cpa.operators.DocumentProcessor;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiDocument;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * Number of see also links
 * Number of out/inbound links
 * Number of words in article
 * Avg. distance of links in article
 * Number of headline in article
 */
public class ArticleStats {
    public static int getQuartileOfArticleLength(int length) {
        // borders
        int q1 = 178;
        int q2 = 362;
        int q3 = 774;

        if (length < q1)
            return 1;
        else if (length < q2)
            return 2;
        else if (length < q3)
            return 3;
        else
            return 4;

    }

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length < 2) {
            System.err.println("Input/output parameters missing!");
            System.err.println("Arguments: [WIKISET] [OUTPUT-LIST] [OUTPUT-STATS]");
            System.exit(1);
        }

        String inputWikiSet = args[0];
        String outputListFilename = args[1];


        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputWikiSet);

        // ArticleCounter, Links (, AvgDistance
        DataSet<ArticleTuple> articleDataSet = text.flatMap(new FlatMapFunction<String, ArticleTuple>() {
            public void flatMap(String content, Collector out) {

                WikiDocument doc = new DocumentProcessor().processDoc(content);
                if (doc == null) return;

                int words = doc.getWordMap().size();
                int outLinks = doc.getOutLinks().size();
                int headlines = doc.getHeadlines().size();
                double avgLinkDistance = doc.getAvgLinkDistance();
                double outLinksPerWords = ((double) outLinks) / ((double) words);

                out.collect(new ArticleTuple(
                                doc.getTitle(),
                                words,
                                headlines,
                                outLinks,
                                avgLinkDistance,
                                outLinksPerWords
                        )

                );

            }
        });

        if (outputListFilename.equals("print")) {
            articleDataSet.print();
        } else {
            articleDataSet.writeAsCsv(outputListFilename, "\n", "|", FileSystem.WriteMode.OVERWRITE);
        }


////        DataSet<Tuple2<String, String>> output = env.readFile(new GenericCsvDelimitedInputFormat<Tuple2<String, String>>(), inputLinkSet);
//        DataSet<LinkResult> links = env.readFile(new LinksResultInputFormat(), inputLinkSet);
//
//        DataSet<Tuple2<String, Integer>> outboundLinks = links
//                .groupBy(0)
//                .reduceGroup(new GroupReduceFunction<LinkResult, Tuple2<String, Integer>>() {
//                    @Override
//                    public void reduce(Iterable<LinkResult> linkResults, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        Iterator<LinkResult> iterator = linkResults.iterator();
//                        LinkResult record = null;
//                        int i = 0;
//                        while (iterator.hasNext()) {
//                            record = iterator.next();
//                            i++;
//                        }
//                        out.collect(new Tuple2<String, Integer>((String) record.getField(0), i));
//                    }
//                });
//
//        DataSet<Tuple2<String, Integer>> inboundLinks = links
//                .groupBy(1)
//                .reduceGroup(new GroupReduceFunction<LinkResult, Tuple2<String, Integer>>() {
//                    @Override
//                    public void reduce(Iterable<LinkResult> linkResults, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        Iterator<LinkResult> iterator = linkResults.iterator();
//                        LinkResult record = null;
//                        int i = 0;
//                        while (iterator.hasNext()) {
//                            record = iterator.next();
//                            i++;
//                        }
//                        out.collect(new Tuple2<String, Integer>((String) record.getField(1), i));
//                    }
//                });
//
//        DataSet<Tuple1<String>> seeAlsoResults = env.readFile(new SeeAlsoResultInputFormat(), inputSeeAlsoSet)
//                .project(0);
//
//        seeAlsoResults = seeAlsoResults.distinct(0);
//
//        DataSet<Tuple5<String, Integer, Integer, Integer, Integer>> output = seeAlsoResults
//                .coGroup(inboundLinks)
//                .where(0)
//                .equalTo(0)
//                .with(new CoGroupFunction<Tuple1<String>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//
//                    @Override
//                    public void coGroup(Iterable<Tuple1<String>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        Iterator<Tuple1<String>> iterator1 = first.iterator();
//                        Iterator<Tuple2<String, Integer>> iterator2 = second.iterator();
//
//                        Tuple1<String> record = null;
//                        Tuple2<String, Integer> join = null;
//
//
//                        if (iterator1.hasNext()) {
//                            record = iterator1.next();
//                            int i = 0;
//
//                            if (iterator2.hasNext()) {
//                                join = iterator2.next();
//                                i = join.getField(1);
//
//                                System.out.println("##" + join.toString());
//                            }
//
//                            out.collect(new Tuple2<String, Integer>((String) record.getField(0), i));
//                        }
//                    }
//                })
//                .coGroup(outboundLinks)
//                .where(0)
//                .equalTo(0)
//                .with(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
//                    @Override
//                    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
//                        Iterator<Tuple2<String, Integer>> iterator1 = first.iterator();
//                        Iterator<Tuple2<String, Integer>> iterator2 = second.iterator();
//
//                        Tuple2<String, Integer> record = null;
//                        Tuple2<String, Integer> join = null;
//
//
//                        if (iterator1.hasNext()) {
//                            record = iterator1.next();
//                            int i = 0;
//
//                            if (iterator2.hasNext()) {
//                                join = iterator2.next();
//                                i = join.getField(1);
//                            }
//
//                            out.collect(new Tuple3<>((String) record.getField(0), (int) record.getField(1), i));
//                        }
//                    }
//                })
//                .coGroup(articleLength)
//                .where(0)
//                .equalTo(0)
//                .with(new CoGroupFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>, Tuple5<String, Integer, Integer, Integer, Integer>>() {
//                    @Override
//                    public void coGroup(Iterable<Tuple3<String, Integer, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple5<String, Integer, Integer, Integer, Integer>> out) throws Exception {
//                        Iterator<Tuple3<String, Integer, Integer>> iterator1 = first.iterator();
//                        Iterator<Tuple2<String, Integer>> iterator2 = second.iterator();
//
//                        Tuple3<String, Integer, Integer> record = null;
//                        Tuple2<String, Integer> join = null;
//
//
//                        if (iterator1.hasNext()) {
//                            record = iterator1.next();
//                            int i = 0;
//
//                            if (iterator2.hasNext()) {
//                                join = iterator2.next();
//                                i = join.getField(1);
//                            }
//
//                            out.collect(new Tuple5<>((String) record.getField(0), (int) record.getField(1), (int) record.getField(2), i, getQuartileOfArticleLength(i)));
//                        }
//                    }
//                });
//
////        inboundLinks.print();
//
//
//        DataSet<Tuple2<String, Integer>> oxutput = inboundLinks
//                .union(outboundLinks)
//                .union(articleLength);
//
//        if (outputFilename.equals("print")) {
//            output.print();
//        } else {
//            output.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, WikiSimConfiguration.csvFieldDelimiter, FileSystem.WriteMode.OVERWRITE);
//        }

        env.execute("ArticleStats");
    }
}
