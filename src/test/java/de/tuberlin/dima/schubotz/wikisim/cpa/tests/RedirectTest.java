package de.tuberlin.dima.schubotz.wikisim.cpa.tests;


import de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils.Tester;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.LinkTuple;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.list.DoubleListValue;
import de.tuberlin.dima.schubotz.wikisim.cpa.utils.StringUtils;
import de.tuberlin.dima.schubotz.wikisim.redirects.RedirectCount;
import de.tuberlin.dima.schubotz.wikisim.redirects.RedirectExtractor;
import de.tuberlin.dima.schubotz.wikisim.redirects.SeeAlsoRedirects;
import de.tuberlin.dima.schubotz.wikisim.redirects.single.WikiSimRedirects;
import de.tuberlin.dima.schubotz.wikisim.redirects.single.WikiSimRedirectsResult;
import org.apache.flink.types.DoubleValue;
import org.junit.Ignore;
import org.junit.Test;

public class RedirectTest extends Tester {

    @Ignore
    @Test
    public void TestWikiSimRedirects() throws Exception {

        WikiSimRedirects.main(
                new String[]{
//                        "dataset",
                        resource("testresult2.csv"),
                        resource("redirects.out"),
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
    @Test
    public void RedirectEncoding() throws Exception {
        RedirectExtractor.main(new String[]{
                "/Users/malteschwarzer/Desktop/wikitest.xml",
                "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath()
        });
    }

    @Ignore
    @Test
    public void RedirectionExecution() throws Exception {

        RedirectExtractor.main(new String[]{
                resource("wikiRedirect.xml"),
//               "print" //outputFilename
                resource("redirects.out")

        });
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
    public void TestResultConstructor() {
        WikiSimResult r1 = new WikiSimResult(new LinkTuple("Page A", "Page B"), 99);
        r1.setCPA(new double[]{0.1, 0.5, 1.5, 24.5, 88});
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
