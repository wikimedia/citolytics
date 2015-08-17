package de.tuberlin.dima.schubotz.wikisim.cpa.tests;

import de.tuberlin.dima.schubotz.wikisim.cpa.operators.DocumentProcessor;
import de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils.Tester;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.wikisim.linkgraph.LinkGraph;
import de.tuberlin.dima.schubotz.wikisim.stats.ArticleStats;
import de.tuberlin.dima.schubotz.wikisim.stats.ArticleStatsWithInboundLinks;
import org.junit.Ignore;
import org.junit.Test;


public class ArticleStatsTest extends Tester {
    @Ignore
    @Test
    public void LocalExecution() throws Exception {

        ArticleStats.main(new String[]{
                resource("wikiSeeAlso2.xml"),
                "print" //outputFilename
        });
    }

    @Ignore
    @Test
    public void LocalExecutionWithInboundLinks() throws Exception {

        ArticleStatsWithInboundLinks.main(new String[]{
                input("completeTestWikiDump.xml"),
                "print" //outputFilename
        });
    }

    @Test
    public void HeadlineTest() {


        String xml = getFileContents("wikiSeeAlso.xml");

        WikiDocument doc = new DocumentProcessor().processDoc(xml);

        System.out.println("Headlines: " + doc.getHeadlines().size());

    }

    @Test
    public void AvgLinkDistanceTest() {


        String xml = getFileContents("wikiSeeAlso.xml");

        WikiDocument doc = new DocumentProcessor().processDoc(xml);

        System.out.println("AvgLinkDistance: " + doc.getAvgLinkDistance());

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
}
