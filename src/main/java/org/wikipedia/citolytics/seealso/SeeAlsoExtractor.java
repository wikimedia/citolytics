package org.wikipedia.citolytics.seealso;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cirrussearch.IdTitleMappingExtractor;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.IdTitleMapping;
import org.wikipedia.citolytics.cpa.types.LinkPair;
import org.wikipedia.citolytics.cpa.types.LinkPairWithIds;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;
import org.wikipedia.citolytics.multilang.LangLinkTuple;
import org.wikipedia.citolytics.multilang.MultiLang;
import org.wikipedia.citolytics.redirects.single.SeeAlsoRedirects;
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
 *
 * TODO Also read from template https://en.wikipedia.org/wiki/Template:See_also
 * TODO Something the section may be called "Related pages" https://simple.wikipedia.org/wiki/Wikipedia:Manual_of_Style#Related_pages
 */
public class SeeAlsoExtractor extends WikiSimAbstractJob<Tuple3<String, String, Integer>> {
    public final static String SEEALSO_LINK_DELIMITER = "#";

    public static void main(String[] args) throws Exception {
        new SeeAlsoExtractor().start(args);
    }

    public void plan() throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        jobName = "SeeAlsoExtractor";

        String inputFilename = params.getRequired("input");
        outputFilename = params.getRequired("output");

        // Read Wikipedia XML Dump
        DataSource<String> wikiDump = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        // Multi-language support
        String langLinksPath = params.get("lang-links");
        String inputLang = params.get("input-lang");

        DocumentProcessor dp = new DocumentProcessor();

        if(inputLang != null) {
            dp.setSeeAlsoTitleByLanguage(inputLang);
        }

        // Extract "See also" links from Wikipedia dump
        DataSet<LinkPair> seeAlsolinkPairs = wikiDump.flatMap(new Extractor(dp));


        if(inputLang != null) {
            // Load intermediate data sets (output is enwiki)
            DataSet<IdTitleMapping> idTitleMappings = IdTitleMappingExtractor.extractIdTitleMapping(env, wikiDump);
            DataSet<LangLinkTuple> langLinks = MultiLang.readLangLinksDataSet(env, langLinksPath, "en");

//            System.out.println(">>> SeeAlso link pairs = " + seeAlsolinkPairs.collect());

            // Find page ids for source and target titles
            DataSet<LinkPairWithIds> linkPairsWithIds = seeAlsolinkPairs
                    .leftOuterJoin(idTitleMappings)
                    .where(LinkPairWithIds.PAGE_A_TITLE_KEY)
                    .equalTo(IdTitleMapping.TITLE_KEY)
                    .with(new JoinFunction<LinkPair, IdTitleMapping, LinkPairWithIds>() {
                        @Override
                        public LinkPairWithIds join(LinkPair linkPair, IdTitleMapping idTitleMapping) throws Exception {
                            return new LinkPairWithIds(
                                    linkPair.getFirst(),
                                    linkPair.getSecond(),
                                    idTitleMapping.f0,
                                    0
                            );
                        }
                    });

//            System.out.println(">> with ids link pairs 1 = " + linkPairsWithIds.collect());
//            System.out.println(">> id titles = " + idTitleMappings.collect());

            linkPairsWithIds = linkPairsWithIds
                    .leftOuterJoin(idTitleMappings)
                    .where(LinkPairWithIds.PAGE_B_TITLE_KEY)
                    .equalTo(IdTitleMapping.TITLE_KEY)
                    .with(new FlatJoinFunction<LinkPairWithIds, IdTitleMapping, LinkPairWithIds>() {
                        @Override
                        public void join(LinkPairWithIds linkPair, IdTitleMapping idTitleMapping, Collector<LinkPairWithIds> out) throws Exception {
                            if(idTitleMapping != null) {
                                out.collect(new LinkPairWithIds(
                                        linkPair.f0,
                                        linkPair.f1,
                                        linkPair.f2,
                                        idTitleMapping.getField(IdTitleMapping.ID_KEY)
                                ));
                            }
                        }
                    });

//            System.out.println(">> with ids link pairs 2 = " + linkPairsWithIds.collect());
//            System.out.println(">> lang links = " + langLinks.collect());

            // Translate via page ids to page titles (source and target pages)
            seeAlsolinkPairs = linkPairsWithIds
                    .join(langLinks)
                    .where(LinkPairWithIds.PAGE_A_ID_KEY)
                    .equalTo(LangLinkTuple.PAGE_ID_KEY)
                    .with(new JoinFunction<LinkPairWithIds, LangLinkTuple, LinkPairWithIds>() {
                        @Override
                        public LinkPairWithIds join(LinkPairWithIds a, LangLinkTuple b) throws Exception {
                            return new LinkPairWithIds(
                                    b.getTargetTitle(),
                                    a.f1,
                                    a.f2,
                                    a.f3
                            );
                        }
                    })
                    .join(langLinks)
                    .where(LinkPairWithIds.PAGE_B_ID_KEY)
                    .equalTo(LangLinkTuple.PAGE_ID_KEY)
                    .with(new JoinFunction<LinkPairWithIds, LangLinkTuple, LinkPair>() {

                        @Override
                        public LinkPair join(LinkPairWithIds a, LangLinkTuple b) throws Exception {
                            return new LinkPair(
                                    a.f0,
                                    b.getTargetTitle()
                            );
                        }
                    });

        }

        if (!params.has("redirects")) {
            // Extract SeeAlso links (no redirect resolving)
            result = getSeeAlsoOutput(seeAlsolinkPairs);
        } else {
            // Resolve redirects in SeeAlso links
            jobName += " with redirects";

            DataSet<RedirectMapping> redirects = WikiSimRedirects.getRedirectsDataSet(env, params.get("redirects"));

            result = SeeAlsoRedirects.resolveRedirects(seeAlsolinkPairs, redirects);
        }

    }

    class Extractor implements FlatMapFunction<String, LinkPair> {
        DocumentProcessor dp;

        public Extractor(DocumentProcessor dp) {
            this.dp = dp;
        }

        @Override
        public void flatMap(String content, Collector<LinkPair> out) throws Exception {
            WikiDocument doc = dp.processDoc(content, true);

            // Valid article?
            if (doc == null) return;

            List<Map.Entry<String, Integer>> links = doc.getOutLinks();

            // SeeAlso links exist?
            if (links.size() < 1) return;

            for (Map.Entry<String, Integer> outLink : links) {
                out.collect(new LinkPair(doc.getTitle(), outLink.getKey()));
            }
        }
    };


    public static DataSet<Tuple3<String, String, Integer>> getSeeAlsoOutput(DataSet<LinkPair> seeAlsolinkPairs) {
        return seeAlsolinkPairs
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(new GroupReduceFunction<LinkPair, Tuple3<String, String, Integer>>() {
                    @Override
                    public void reduce(Iterable<LinkPair> in, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        Iterator<LinkPair> iterator = in.iterator();
                        String source = null;
                        String linkTargets = null;
                        int linksCount = 0;

                        while(iterator.hasNext()) {
                            LinkPair link = iterator.next();

                            if(source == null) {
                                source = link.getFirst();
                            }

                            if(linkTargets == null) {
                                linkTargets = link.getSecond();
                            } else {
                                linkTargets += SeeAlsoExtractor.SEEALSO_LINK_DELIMITER + link.getSecond();
                            }

                            linksCount++;
                        }

                        if(linksCount > 0) {
                            out.collect(new Tuple3<>(source, linkTargets, linksCount));
                        }
                    }
                });
    }

}
