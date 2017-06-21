package org.wikipedia.citolytics.tests;

import org.junit.Test;
import org.wikipedia.citolytics.stats.ArticleStats;
import org.wikipedia.citolytics.tests.utils.Tester;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import static org.junit.Assert.assertEquals;


public class ArticleStatsTest extends Tester {
    @Test
    public void testWriteToFile() throws Exception {

        ArticleStats job = new ArticleStats();

        job
                .enableTestEnvironment()
                .start("--wikidump " + input("ArticleStatsTest/completeTestWikiDump.xml")
                        + " --in-links"
                        + " --output " + output("ArticleStatsTest/stats.out"));

    }

    @Test
    public void testSummary() throws Exception {

        ArticleStats job = new ArticleStats();

        job.enableTestEnvironment()
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

        job.enableTestEnvironment()
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
    }

    @Test
    public void testSummaryWithInboundLinksWithoutRedirects() throws Exception {
        // Without redirects
        ArticleStats job = new ArticleStats();
        job.enableTestEnvironment()
            .start("--wikidump " + input("ArticleStatsTest/completeTestWikiDump.xml")
                        + " --output local"
                        + " --summary --in-links");

//        assertJobOutputStringWithFixture();
        assertEquals("Invalid in link count (without redirects)", 22, job.getOutput().get(0).getInLinks());
    }


    @Test
    public void testInboundLinks() throws Exception {
        ArticleStats job = new ArticleStats();
        job.enableTestEnvironment()
            .start("--wikidump " + input("ArticleStatsTest/completeTestWikiDump.xml")
                + " --output local"
                + " --in-links");

        assertJobOutputStringWithResource(job, "ArticleStatsTest/inbound_links.expected", "Invalid inbounds links");
//        System.out.println(job.getOutput());
    }

    @Test
    public void testHeadlineCount() {

        String xml = getFileContents("ArticleStatsTest/wikiSeeAlso.xml");

        WikiDocument doc = new DocumentProcessor().processDoc(xml);

        assertEquals("Invalid headline count", 39, doc.getHeadlines().size());

    }

    @Test
    public void testAvgLinkDistance() {

        String xml = getFileContents("ArticleStatsTest/wikiSeeAlso.xml");

        WikiDocument doc = new DocumentProcessor().processDoc(xml);
        // old invalid namespace check=4121.20
        assertEquals("AvgLinkDistance is wrong", 4120.99, doc.getAvgLinkDistance(), 0.01);

    }

}
