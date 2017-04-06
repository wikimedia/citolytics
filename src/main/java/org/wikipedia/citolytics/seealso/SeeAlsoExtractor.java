package org.wikipedia.citolytics.seealso;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;
import org.wikipedia.citolytics.redirects.single.WikiSimRedirects;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Extracts SeeAlso links from Wikipedia articles and creates CSV for DB import.
 * <p/>
 * Output format: article name | SeeAlso link 1#SeeAlso link2#... | number of SeeAlso links
 */
public class SeeAlsoExtractor extends WikiSimAbstractJob<Tuple3<String, String, Integer>> {
    private final String linkDelimiter = "#";

    public static void main(String[] args) throws Exception {
        new SeeAlsoExtractor().start(args);
    }

    public void plan() {
        ParameterTool params = ParameterTool.fromArgs(args);
        jobName = "SeeAlsoExtractor";

        String inputFilename = params.getRequired("input");
        outputFilename = params.getRequired("output");

        // Read Wikipedia XML Dump
        DataSource<String> wikiPages = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        if (!params.has("redirects")) {
            // Extract SeeAlso links (no redirect resolving)
            result = wikiPages.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
                public void flatMap(String content, Collector out) {

                    WikiDocument doc = new DocumentProcessor().processDoc(content, true);

                    // Valid article?
                    if (doc == null) return;

                    List<Map.Entry<String, Integer>> links = doc.getOutLinks();

                    // SeeAlso links exist?
                    if (links.size() < 1) return;

                    String linkNames = null;
                    for (Map.Entry<String, Integer> outLink : links) {
                        if (linkNames == null) {
                            linkNames = outLink.getKey();
                        } else {
                            linkNames += linkDelimiter + outLink.getKey();
                        }
                    }

                    out.collect(new Tuple3<>(doc.getTitle(), linkNames, links.size()));
                }
            });
        } else {
            // Resolve redirects in SeeAlso links
            jobName += " with redirects";

            DataSet<RedirectMapping> redirects = WikiSimRedirects.getRedirectsDataSet(env, params.get("redirects"));

            result = wikiPages.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                @Override
                public void flatMap(String content, Collector<Tuple2<String, String>> out) throws Exception {

                    WikiDocument doc = new DocumentProcessor().processDoc(content, true);

                    // Valid article?
                    if (doc == null) return;

                    List<Map.Entry<String, Integer>> links = doc.getOutLinks();

                    // SeeAlso links exist?
                    if (links.size() < 1) return;

                    for (Map.Entry<String, Integer> outLink : links) {
                        out.collect(new Tuple2<>(doc.getTitle(), outLink.getKey()));
                    }
                }
            })
                    .coGroup(redirects)
                    .where(1) // see also link
                    .equalTo(0) // redirect source
                            // replace
                    .with(new CoGroupFunction<Tuple2<String, String>, RedirectMapping, Tuple2<String, String>>() {
                        @Override
                        public void coGroup(Iterable<Tuple2<String, String>> seeAlso, Iterable<RedirectMapping> redirect, Collector<Tuple2<String, String>> out) throws Exception {
                            Iterator<Tuple2<String, String>> iteratorSeeAlso = seeAlso.iterator();
                            Iterator<RedirectMapping> iteratorRedirect = redirect.iterator();

                            while (iteratorSeeAlso.hasNext()) {
                                Tuple2<String, String> recordSeeAlso = iteratorSeeAlso.next();

                                if (iteratorRedirect.hasNext()) {
                                    RedirectMapping recordRedirect = iteratorRedirect.next();

                                    // replace
                                    recordSeeAlso.setField(recordRedirect.getTarget(), 1);
                                }
                                out.collect(recordSeeAlso);
                            }
                        }
                    })
                            // group back to see also structure
                    .groupBy(0)
                    .reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>() {
                        @Override
                        public void reduce(Iterable<Tuple2<String, String>> in, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                            Iterator<Tuple2<String, String>> iterator = in.iterator();
                            String article = null;
                            String seeAlsoLinks = null;
                            int counter = 0;

                            while (iterator.hasNext()) {
                                Tuple2<String, String> link = iterator.next();
                                if (article == null) {
                                    article = link.f0;
                                    seeAlsoLinks = link.f1;
                                } else {
                                    seeAlsoLinks += "#" + link.f1;
                                }
                                counter++;
                            }

                            out.collect(new Tuple3<>(article, seeAlsoLinks, counter));
                        }
                    });
        }

    }

}
