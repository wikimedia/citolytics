package org.wikipedia.citolytics.tests;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.DoubleValue;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.cpa.WikiSim;
import org.wikipedia.citolytics.cpa.types.LinkPair;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.types.list.DoubleListValue;
import org.wikipedia.citolytics.cpa.utils.CheckOutputIntegrity;
import org.wikipedia.citolytics.cpa.utils.WikiSimStringUtils;
import org.wikipedia.citolytics.redirects.RedirectCount;
import org.wikipedia.citolytics.redirects.RedirectExtractor;
import org.wikipedia.citolytics.redirects.single.SeeAlsoRedirects;
import org.wikipedia.citolytics.redirects.single.WikiSimRedirects;
import org.wikipedia.citolytics.redirects.single.WikiSimRedirectsResult;
import org.wikipedia.citolytics.tests.utils.Tester;

import static org.junit.Assert.assertEquals;

/**
 * Tests for redirects.* package and WikiSim with redirects
 */
public class RedirectsTest extends Tester {
    @Test
    public void testRedirectCount() throws Exception {
        RedirectCount job = new RedirectCount();

        job
                .enableLocalEnvironment()
                .start("--output local"
                                + " --redirects " + resource("redirects.in", true)
                                + " --links " + resource("links.in", true));

        assertEquals("Invalid count returned", 1, (int) job.output.get(0).getField(0));
    }

    @Test
    public void testRedirectExtractor() throws Exception {
        RedirectExtractor job = new RedirectExtractor();

        job.enableLocalEnvironment()
                .start("--output local --input " + resource("wiki_dump.xml.in", true));

        assertEquals("Invalid redirect returned", new Tuple2<>("Games", "Game"), job.output.get(0));
        assertEquals("Invalid number of redirects returned", 1, job.output.size());
    }


    @Test
    public void TestRedirectedExecution() throws Exception {

        WikiSim job = new WikiSim();

        job.enableLocalEnvironment().start(("--input " + input("wikiRedirectedLinks.xml")
                + " --output print"
                + " --alpha 1.5,1.75 --redirects " + input("ArticleStatsTest/redirects.csv")).split(" "));

    }


    @Test
    public void TestIntegrity() throws Exception {
        String outputA = "wikisim_integrity_a.out";
        String outputB = "wikisim_integrity_b.out";
        WikiSim job = new WikiSim();

        job.enableLocalEnvironment();

        job.start(("--input " + input("ArticleStatsTest/completeTestWikiDump.xml")
                + " --output " + output(outputA)
                + " --alpha 1.5,1.75 --redirects " + input("ArticleStatsTest/redirects.csv")).split(" "));

        job.start(("--input " + input("ArticleStatsTest/completeTestWikiDump.xml")
                + " --output " + output(outputB)
                + " --alpha 1.5,1.75 --redirects " + input("ArticleStatsTest/redirects.csv")).split(" "));


        CheckOutputIntegrity test = new CheckOutputIntegrity();

        test.enableLocalEnvironment().start(new String[]{
                resource(outputA),
                resource(outputB),
                "local"
        });

        assertEquals("CheckOutputIntegrity results should be empty.", 0, test.output.size());

    }


    @Ignore
    @Test
    public void TestWikiSimRedirects() throws Exception {

        WikiSimRedirects.main(
                new String[]{
//                        "dataset",
                        resource("wikisim_output.csv"),
                        resource("ArticleStatsTest/redirects.csv"),
                        "print"
                }
        );
    }

    @Ignore
    @Test
    public void TestSeeAlsoRedirects() throws Exception {
        SeeAlsoRedirects.main(
                new String[]{
                        resource("evaluation_seealso.csv"),
                        resource("redirects.out"),
                        "print"
                }
        );
    }


    @Ignore
//    @Test
    public void RedirectEncoding() throws Exception {

        RedirectExtractor job = new RedirectExtractor();

        job.start(new String[]{
                "/Users/malteschwarzer/Desktop/wikitest.xml",
                "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath()
        });
    }

    @Ignore
    @Test
    public void RedirectionExecution() throws Exception {

//        RedirectExtractor.main(("--input " + resource("wikiRedirect.xml") + " --output " + resource("redirects.out")).split(" "));
        RedirectExtractor.main(("--input " + resource("wikiRedirect.xml") + " --output print").split(" "));

    }

    @Ignore
    @Test
    public void RedirectionCount() throws Exception {


        RedirectCount.main(new String[]{
                "file://" + getClass().getClassLoader().getResources("linkGraphInput.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath(),

                "print" //outputFilename
        });
    }

    @Ignore
    @Test
    public void TestResultConstructor() {
        RecommendationPair r1 = new RecommendationPair(new LinkPair("Page A", "Page B"), 99);
        r1.setCPI(new double[]{0.1, 0.5, 1.5, 24.5, 88});
        r1.setDistSquared(500);

        System.out.println("r1 = " + r1);

        WikiSimRedirectsResult r2 = new WikiSimRedirectsResult(r1);

        System.out.println("r2 = " + r2);

    }

    @Test
    public void DoubleListSumA() throws Exception {
        DoubleListValue a = DoubleListValue.valueOf(new double[]{1, 10, 100});
        DoubleListValue b = DoubleListValue.valueOf(new double[]{2, 20, 200});

        DoubleListValue sum = DoubleListValue.sum(a, b);

        if (sum.get(0).getValue() != 3
                || sum.get(1).getValue() != 30
                || sum.get(2).getValue() != 300) {
            throw new Exception("DoubleListValue.sum() does not work: " + sum);
        }
    }

    // double is not accurate !!!
    public void DoubleListSumB() {
        System.out.println(DoubleListValue.valueOf(new double[]{0.1 + 0.2}));
        System.out.println(new DoubleValue(0.3));

        double d = 0.1 + 0.2;
        System.out.println(d);
    }

    @Test
    public void unescapeEntitiesTest() throws Exception {
        String s = "Usinger|Usinger&#039;s"
                + "\nChūbu"
                + "\n中部地方"
                + "\n&amp;";

        String unescaped = WikiSimStringUtils.unescapeEntities(s);

        if (unescaped.indexOf("&amp;") > -1 || unescaped.indexOf("&#039;") > -1 || s.equals(unescaped)) {
            throw new Exception("Unescape failed: " + unescaped);
        }

        //System.out.println(" B = " + StringEscapeUtils.unescapeHtml4(s));
    }


}
