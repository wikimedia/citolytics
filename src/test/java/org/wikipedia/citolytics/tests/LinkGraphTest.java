package org.wikipedia.citolytics.tests;

import org.junit.Test;
import org.wikipedia.citolytics.linkgraph.LinkGraph;
import org.wikipedia.citolytics.linkgraph.LinksExtractor;
import org.wikipedia.citolytics.tests.utils.Tester;

import static org.junit.Assert.assertEquals;

/**
 * Tests for linkgraph.* package
 */
public class LinkGraphTest extends Tester {
    @Test
    public void testRedirectsInLinkGraph() throws Exception {
        LinkGraph job = new LinkGraph();

        job.enableLocalEnvironment()
                .start(new String[]{
                        input("ArticleStatsTest/completeTestWikiDump.xml"),
                        input("ArticleStatsTest/redirects.csv"),
                        input("linkGraphInput.csv"),
                        "local"
                });

        assertEquals("Invalid number of link graph items returned", 3, job.output.size());
    }


    @Test
    public void testLinksExtractor() throws Exception {
        LinksExtractor job = new LinksExtractor();

        job.enableLocalEnvironment().start("--input " + input("ArticleStatsTest/linkParserTest.xml")
                + " --output local");
        assertEquals("Invalid link count", 194, job.output.size()); // old namespace check = 195
    }
}
