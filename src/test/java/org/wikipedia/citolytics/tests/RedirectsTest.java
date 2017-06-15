package org.wikipedia.citolytics.tests;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.cpa.WikiSim;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.utils.CheckOutputIntegrity;
import org.wikipedia.citolytics.cpa.utils.WikiSimStringUtils;
import org.wikipedia.citolytics.redirects.RedirectCount;
import org.wikipedia.citolytics.redirects.RedirectExtractor;
import org.wikipedia.citolytics.redirects.single.SeeAlsoRedirects;
import org.wikipedia.citolytics.redirects.single.WikiSimRedirects;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.util.Arrays;
import java.util.List;

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


    @Test
    public void testWikiSimRedirects() throws Exception {
        setJob(new WikiSimRedirects()).start(
                "--wikisim " + resource("wikisim.in", true)
            + " --redirects " + resource("redirects.in", true)
            + " --output local");

        assertEquals("Invalid wikisim output size", 16, job.output.size());
    }


    @Test
    public void testResolveSeeAlsoRedirects() throws Exception {
        setJob(new SeeAlsoRedirects()).start(
                "--seealso " + resource("seealso_links.in", true)
                        + " --redirects " + resource("redirects.in", true)
                        + " --output local"
        );

        assertEquals("Invalid number of records returned", 7, job.output.size());
//        job.output
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


    @Test
    public void testDoubleListSumA() throws Exception {
        List<Double> a = Arrays.asList(1., 10., 100.); //new ArrayList<Double>(){1., 10., 100.}; //Arrays.asList(new double[]{1, 10, 100});
        List<Double> b = Arrays.asList(2., 20., 200.);

        List<Double> sum = RecommendationPair.sum(a, b);

        if (sum.get(0) != 3
                || sum.get(1) != 30
                || sum.get(2) != 300) {
            throw new Exception("DoubleListValue.sum() does not work: " + sum);
        }
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
