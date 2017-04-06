package org.wikipedia.citolytics.stats;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.linkgraph.LinksExtractor;
import org.wikipedia.citolytics.redirects.single.WikiSimRedirects;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import java.util.regex.Pattern;


/**
 * Combine output of ArticleStats and LinksExtractor to add inbound link stats.
 *
 * Usage: --wikidump <WIKI-XML> --output <OUTPUT-FILE> [--summary] [--redirects <REDIRECTS-FILE>]
 */
public class ArticleStats extends WikiSimAbstractJob<ArticleStatsTuple> {
    public boolean summary;
    public boolean inLinks;
    public String inputFilename;
    public String redirectsFilename;

    public static void main(String[] args) throws Exception {
        new ArticleStats().start(args);
    }

    public void init() {
        jobName = "ArticleStats";

        ParameterTool params = ParameterTool.fromArgs(args);

        inputFilename = params.getRequired("wikidump");
        outputFilename = params.getRequired("output");
        redirectsFilename = params.get("redirects");
        summary = params.has("summary");
        inLinks = params.has("in-links");
    }

    public void plan() throws Exception {

        DataSet<ArticleStatsTuple> stats = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename)
                .flatMap(new FlatMapFunction<String, ArticleStatsTuple>() {
                    public void flatMap(String content, Collector out) {
                        collectStats(content, out);
                    }
                });

        if(inLinks) {
            DataSet<ArticleInLinksTuple> links =
                    env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename)
                            .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                                public void flatMap(String content, Collector out) {
                                    LinksExtractor.collectLinks(content, out);
                                }
                            })
                            .distinct() // do not count multiple links from one page
                            .map(new MapFunction<Tuple2<String, String>, ArticleInLinksTuple>() {
                                @Override
                                public ArticleInLinksTuple map(Tuple2<String, String> in) throws Exception {
                                    return new ArticleInLinksTuple(in.f1, 1); // link target
                                }
                            })
                            .groupBy(0) // group by link-target
                            .sum(1) // sum link count
                    ;

            // Resolve redirects in inbound links
            if (redirectsFilename != null) {
                DataSet<RedirectMapping> redirects = WikiSimRedirects.getRedirectsDataSet(env, redirectsFilename);

                links = links
                        .leftOuterJoin(redirects)
                        .where(0)
                        .equalTo(RedirectMapping.SOURCE_KEY)
                        // Replace redirects
                        .with(new JoinFunction<ArticleInLinksTuple, RedirectMapping, ArticleInLinksTuple>() {
                            @Override
                            public ArticleInLinksTuple join(ArticleInLinksTuple articleInLinks, RedirectMapping redirectMapping) throws Exception {
                                if (redirectMapping != null) {
                                    articleInLinks.f0 = redirectMapping.getTarget();
                                }
                                return articleInLinks;
                            }
                        })
                        // Merge duplicates
                        .groupBy(0)
                        .aggregate(Aggregations.SUM, 1)
                ;
            }

            result = stats
                    .leftOuterJoin(links)
                    .where(ArticleStatsTuple.ARTICLE_NAME_KEY)
                    .equalTo(0) // target name
                    .with(new JoinFunction<ArticleStatsTuple, ArticleInLinksTuple, ArticleStatsTuple>() {
                        @Override
                        public ArticleStatsTuple join(ArticleStatsTuple stats, ArticleInLinksTuple inLinks) throws Exception {
                            if (inLinks != null) {
                                stats.setInLinks(inLinks.f1);
                            }
                            return stats;
                        }
                    });
        } else {
            result = stats;
        }

        if(summary) {
            enableSingleOutputFile();

            result = result.sum(ArticleStatsTuple.WORDS_KEY)
                    .andSum(ArticleStatsTuple.HEADLINES_KEY)
                    .andSum(ArticleStatsTuple.OUT_LINKS_KEY)
                    .andSum(ArticleStatsTuple.AVG_LINK_DISTANCE_KEY)
                    .andSum(ArticleStatsTuple.IN_LINKS_KEY);
        }
    }


    public static void collectStats(String content, Collector<ArticleStatsTuple> out) {
        WikiDocument doc = new DocumentProcessor().processDoc(content);
        if (doc == null) return;

        out.collect(new ArticleStatsTuple(
                        doc.getTitle(),
                        doc.getWordMap().size(),
                        doc.getHeadlines().size(),
                        doc.getOutLinks().size(),
                        doc.getAvgLinkDistance()
                )

        );
    }

    public static DataSet<ArticleStatsTuple> getArticleStatsFromFile(ExecutionEnvironment env, String inputFilename) {
        return env.readTextFile(inputFilename).flatMap(new FlatMapFunction<String, ArticleStatsTuple>() {
            @Override
            public void flatMap(String s, Collector<ArticleStatsTuple> out) throws Exception {
                String[] cols = s.split(Pattern.quote(WikiSimConfiguration.csvFieldDelimiter));

                if(cols.length != ArticleStatsTuple.IN_LINKS_KEY + 1)
                    throw new Exception("Invalid article stats row: " + s);

                out.collect(new ArticleStatsTuple(
                        cols[ArticleStatsTuple.ARTICLE_NAME_KEY],
                        Integer.valueOf(cols[ArticleStatsTuple.WORDS_KEY]),
                        Integer.valueOf(cols[ArticleStatsTuple.HEADLINES_KEY]),
                        Integer.valueOf(cols[ArticleStatsTuple.OUT_LINKS_KEY]),
                        Double.valueOf(cols[ArticleStatsTuple.AVG_LINK_DISTANCE_KEY]),
                        Integer.valueOf(cols[ArticleStatsTuple.IN_LINKS_KEY])
                        ));
            }
        });
    }
}
