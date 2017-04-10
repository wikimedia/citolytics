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
        setJob(new LinkGraph())
                .start("--wikidump " + resource("ArticleStatsTest/completeTestWikiDump.xml")
                        + " --redirects " + resource("ArticleStatsTest/redirects.csv")
                        + " --links " + input("linkGraphInput.csv")
                        + " --output local"
                );

        assertEquals("Invalid number of link graph items returned", 3, job.output.size());
    }


    @Test
    public void testLinksExtractor() throws Exception {
        setJob(new LinksExtractor())
                .start("--input " + input("ArticleStatsTest/linkParserTest.xml")
                + " --output local");
        assertEquals("Invalid link count", 194, job.output.size()); // old namespace check = 195
    }
}
