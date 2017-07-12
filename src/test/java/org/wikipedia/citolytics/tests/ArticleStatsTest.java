package org.wikipedia.citolytics.tests;

import org.junit.Test;
import org.wikipedia.citolytics.stats.ArticleStats;
import org.wikipedia.citolytics.stats.CPIAnalysis;
import org.wikipedia.citolytics.stats.CPISampler;
import org.wikipedia.citolytics.stats.LinkDistanceSampler;
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

    @Test
    public void testCPISampler() throws Exception {

        setJob(new CPISampler())
                .start("--input " + resource("cpi_sampler.csv", true)+ " --output local -p 0.3 --page-id-a -1 --page-id-b -1");

//        assertEquals("Invalid sample size", 11, job.getOutput().size());
    }

    @Test
    public void testCPIAnalysis() throws Exception {
        setJob(new CPIAnalysis())
                .start("--top-k 3 --articles \"simple_QQQ,simple_Foo\" --wikisim " + resource("cpi_wikisim_output_lang_simple.csv", true)
                        + " --clickstream " + resource("cpi_clickstream_lang_simple.tsv", true)
                        + " --stats " + resource("cpi_stats_simple.csv", true)
                        + " --lang simple --lang-links " + resource("cpi_lang_links_enwiki.sql", true)
                        + " --id-title-mapping " + resource("cpi_idtitle_mapping.in", true)
                        + " --score 0 --output local"
                        + " --clickstream-output " + resource("cpi_clickstream.out", true));

        assertJobOutputStringWithResource(job, "ArticleStatsTest/cpi_analysis.expected", "Invalid results");
    }

    @Test
    public void testLinkDistanceSample() throws Exception {
        setJob(new LinkDistanceSampler())
                .start("--input " + resource("wikiSeeAlso.xml", true)
                        + " --output " + resource("link_distance_sampler.out", true)
                        + " --p 1.0"
                );

        assertTestResources("Invalid output for absolute proximity",
                "link_distance_sampler.out.abs.expected",
                "link_distance_sampler.out.abs");

        assertTestResources("Invalid output for relative proximity",
                "link_distance_sampler.out.rel.expected",
                "link_distance_sampler.out.rel");

        assertTestResources("Invalid output for structure proximity",
                "link_distance_sampler.out.str.expected",
                "link_distance_sampler.out.str");

    }
}
