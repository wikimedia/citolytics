package org.wikipedia.citolytics.tests;

import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.cpa.types.WikiDocument;
import org.wikipedia.citolytics.linkgraph.LinkGraph;
import org.wikipedia.citolytics.linkgraph.LinksExtractor;
import org.wikipedia.citolytics.stats.ArticleStats;
import org.wikipedia.citolytics.tests.utils.Tester;
import org.wikipedia.processing.DocumentProcessor;

import static org.junit.Assert.assertEquals;


public class ArticleStatsTest extends Tester {
    @Test
    public void testWriteToFile() throws Exception {

        ArticleStats job = new ArticleStats();

        job.silent()
                .enableLocalEnvironment()
                .start("--wikidump " + input("ArticleStatsTest/completeTestWikiDump.xml")
                        + " --in-links"
                        + " --output " + output("ArticleStatsTest/stats.out"));

    }

    @Test
    public void testSummary() throws Exception {

        ArticleStats job = new ArticleStats();

        job.silent()
                .enableLocalEnvironment()
                .start("--wikidump " + input("ArticleStatsTest/completeTestWikiDump.xml")
                        + " --output local"
                        + " --summary");

        assertEquals("Summary should have only single output", 1, job.output.size());
        assertEquals("Invalid word count", 810, job.output.get(0).getWords());
        assertEquals("Invalid headline count", 0, job.output.get(0).getHeadlines());
        assertEquals("Invalid out link count", 24, job.output.get(0).getOutLinks());
        assertEquals("Invalid in link count", 0, job.output.get(0).getInLinks());
    }

    @Test
    public void testSummaryWithInboundLinks() throws Exception {
        /**
         * Article A ---> 3 inbound links
         *           ---> 4 inbound links (with redirects)
         */

        ArticleStats job = new ArticleStats();

        job.silent()
            .enableLocalEnvironment()
                .start("--wikidump " + input("ArticleStatsTest/completeTestWikiDump.xml")
                        + " --output local"
                        + " --redirects " + input("ArticleStatsTest/redirects.csv")
                        + " --summary --in-links");

//        System.out.println(job.output);

        assertEquals("Summary should have only single output", 1, job.output.size());
        assertEquals("Invalid word count", 810, job.output.get(0).getWords());
        assertEquals("Invalid headline count", 0, job.output.get(0).getHeadlines());
        assertEquals("Invalid out link count", 24, job.output.get(0).getOutLinks());
        assertEquals("Invalid in link count", 24, job.output.get(0).getInLinks());

        // Without redirects
        job.start("--wikidump " + input("ArticleStatsTest/completeTestWikiDump.xml")
                        + " --output local"
                        + " --summary --in-links");

        assertEquals("Invalid in link count (without redirects)", 22, job.output.get(0).getInLinks());
    }

    @Test
    public void HeadlineTest() {

        String xml = getFileContents("ArticleStatsTest/wikiSeeAlso.xml");

        WikiDocument doc = new DocumentProcessor().processDoc(xml);

        assertEquals("Invalid headline count", 39, doc.getHeadlines().size());

    }

    @Test
    public void AvgLinkDistanceTest() {

        String xml = getFileContents("ArticleStatsTest/wikiSeeAlso.xml");

        WikiDocument doc = new DocumentProcessor().processDoc(xml);
        // old invalid namespace check=4121.20
        assertEquals("AvgLinkDistance is wrong", 4120.99, doc.getAvgLinkDistance(), 0.01);

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
                        input("ArticleStatsTest/completeTestWikiDump.xml"),
                        input("ArticleStatsTest/redirects.csv"),
                        input("linkGraphInput.csv"),
                        "print"
                });
    }

    @Test
    public void extractLinks() throws Exception {
        LinksExtractor job = new LinksExtractor();

        job.enableLocalEnvironment().start(input("ArticleStatsTest/linkParserTest.xml") + " local");
        assertEquals("Invalid link count", 194, job.output.size()); // old namespace check = 195
    }
}
