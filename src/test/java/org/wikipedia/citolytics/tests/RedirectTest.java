package org.wikipedia.citolytics.tests;


import org.apache.flink.types.DoubleValue;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.cpa.WikiSim;
import org.wikipedia.citolytics.cpa.types.LinkTuple;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;
import org.wikipedia.citolytics.cpa.types.list.DoubleListValue;
import org.wikipedia.citolytics.cpa.utils.CheckOutputIntegrity;
import org.wikipedia.citolytics.cpa.utils.StringUtils;
import org.wikipedia.citolytics.redirects.RedirectCount;
import org.wikipedia.citolytics.redirects.RedirectExtractor;
import org.wikipedia.citolytics.redirects.single.SeeAlsoRedirects;
import org.wikipedia.citolytics.redirects.single.WikiSimRedirects;
import org.wikipedia.citolytics.redirects.single.WikiSimRedirectsResult;
import org.wikipedia.citolytics.tests.utils.Tester;

import static org.junit.Assert.assertEquals;

public class RedirectTest extends Tester {

    @Test
    public void TestRedirectedExecution() throws Exception {

        WikiSim job = new WikiSim();

        job.start(new String[]{
                resource("wikiRedirectedLinks.xml"),
                "print",
                "1.5,1.75", "0", "0", "n",
                resource("redirects.csv")
        });

//        assertEquals("Result count wrong", 3, job.output.size());
//
//        List<WikiSimResult> needles = new ArrayList<>();
//
//        needles.add(new WikiSimResult("Foo", "Redirect target", 17, 2, new double[]{0.08123121086119625, 0.047661356279998054}));
//        needles.add(new WikiSimResult("Bar", "Foo", 9, 2, new double[]{0.40754831530887764, 0.33049721878532895}));
//        needles.add(new WikiSimResult("Bar", "Redirect target", 8, 2, new double[]{0.4215947723372509, 0.3407763504193827}));
//
//        assertTrue("Needles not found.", job.output.containsAll(needles));

//        System.out.println(job.output);
    }


    @Test
    public void TestIntegrity() throws Exception {

        WikiSim job = new WikiSim();

        job.start(new String[]{
                input("completeTestWikiDump.xml"),
                output("wikisim_integrity_a.out"),
                "1.5,1.75", "0", "0", "n",
                input("redirects.csv")
        });


        job.start(new String[]{
                input("completeTestWikiDump.xml"),
                output("wikisim_integrity_b.out"),
                "1.5,1.75", "0", "0", "n",
                input("redirects.csv")
        });


        CheckOutputIntegrity test = new CheckOutputIntegrity();

        test.start(new String[]{
                resource("wikisim_integrity_a.out"),
                resource("wikisim_integrity_b.out"),
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
                        resource("redirects.csv"),
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
        RedirectExtractor.main(new String[]{
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
        WikiSimResult r1 = new WikiSimResult(new LinkTuple("Page A", "Page B"), 99);
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

        String unescaped = StringUtils.unescapeEntities(s);

        if (unescaped.indexOf("&amp;") > -1 || unescaped.indexOf("&#039;") > -1 || s.equals(unescaped)) {
            throw new Exception("Unescape failed: " + unescaped);
        }

        //System.out.println(" B = " + StringEscapeUtils.unescapeHtml4(s));
    }


}
