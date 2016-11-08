package org.wikipedia.citolytics.tests;

import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.cpa.types.WikiDocument;
import org.wikipedia.citolytics.linkgraph.LinkGraph;
import org.wikipedia.citolytics.linkgraph.LinksExtractor;
import org.wikipedia.citolytics.stats.ArticleStats;
import org.wikipedia.citolytics.stats.ArticleStatsWithInboundLinks;
import org.wikipedia.citolytics.tests.utils.Tester;
import org.wikipedia.processing.DocumentProcessor;

import static org.junit.Assert.assertEquals;


public class ArticleStatsTest extends Tester {
    @Ignore
    @Test
    public void LocalExecution() throws Exception {

        new ArticleStats()
                .verbose()
                .start(
                        new String[]{
                                resource("wikiSeeAlso2.xml"),
                                "print" //outputFilename
                        });
    }

    @Ignore
    @Test
    public void LocalExecutionWithInboundLinks() throws Exception {
        /**
         * Article A ---> 3 inbound links
         *           ---> 4 inbound links (with redirects)
         */

        new ArticleStatsWithInboundLinks()
                .verbose()
                .start(new String[]{
                input("completeTestWikiDump.xml"),
                "print" //outputFilename
                        , input("redirects.csv")
        });
    }

    @Test
    public void HeadlineTest() {


        String xml = getFileContents("wikiSeeAlso.xml");

        WikiDocument doc = new DocumentProcessor().processDoc(xml);

        assertEquals("Invalid headline count", 39, doc.getHeadlines().size());

    }

    @Test
    public void AvgLinkDistanceTest() {


        String xml = getFileContents("wikiSeeAlso.xml");

        WikiDocument doc = new DocumentProcessor().processDoc(xml);

        assertEquals("AvgLinkDistance is wrong", 4121.20, doc.getAvgLinkDistance(), 0.01);

    }

    @Ignore
    @Test
    public void TestLinkGraph() throws Exception {

        LinkGraph.main(new String[]{
                resource("wikiSeeAlso2.xml"),
                resource("redirects.out"),
                resource("linkGraphInput.csv"),
                "print"
        });
    }

    @Ignore
    @Test
    public void RedirectsInLinkGraph() throws Exception {
        new LinkGraph()
                .start(new String[]{
                        input("completeTestWikiDump.xml"),
                        input("redirects.csv"),
                        input("linkGraphInput.csv"),
                        "print"
                });
    }

    @Test
    public void extractLinks() throws Exception {
        LinksExtractor job = new LinksExtractor();

        job.start(input("linkParserTest.xml") + " local");
        assertEquals("Invalid link count", 195, job.output.size());
    }
}
