package de.tuberlin.dima.schubotz.wikisim.cpa.tests;


import de.tuberlin.dima.schubotz.wikisim.cpa.types.LinkTuple;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.list.DoubleListValue;
import de.tuberlin.dima.schubotz.wikisim.cpa.utils.StringUtils;
import de.tuberlin.dima.schubotz.wikisim.redirects.RedirectCount;
import de.tuberlin.dima.schubotz.wikisim.redirects.RedirectExtractor;
import de.tuberlin.dima.schubotz.wikisim.redirects.SeeAlsoRedirects;
import de.tuberlin.dima.schubotz.wikisim.redirects.single.WikiSimRedirects;
import de.tuberlin.dima.schubotz.wikisim.redirects.single.WikiSimRedirectsResult;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.types.DoubleValue;
import org.junit.Test;

import java.util.regex.Pattern;

public class RedirectTest {

    @Test
    public void TestWikiSimRedirects() throws Exception {

//        DataSet<WikiSimRedirectResult> input = WikiSimRedirects.env.fromElements(
//            new WikiSimRedirectResult("12232|Afghanistan|Technical University of Berlin|1|1|0.1|0.1"),
//            new WikiSimRedirectResult("12233|Afghanistan|Technical University of Berlin|1|1|0.1|0.1"),
//
//            new WikiSimRedirectResult("12234|Foo|Redirect article|1|2|1.0|1.5"),
//            new WikiSimRedirectResult("12235|Foo|Redirect article 2|1|4|1.0|1.5"),
//            new WikiSimRedirectResult("12236|Redirect article 2|Foo|1|3|1.0|1.5")
//        );
//
//        WikiSimRedirects.wikiSimDataSet = input;
        WikiSimRedirects.main(
                new String[]{
//                        "dataset",
                        "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath(),
                        "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath(),
                        "print"
                }
        );
    }

    @Test
    public void TestSeeAlsoRedirects() throws Exception {
        SeeAlsoRedirects.main(
                new String[]{
                        "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath(),
                        "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath(),
                        "print"
                }
        );
    }

    @Test
    public void RedirectEncoding() throws Exception {
        RedirectExtractor.main(new String[]{
                "/Users/malteschwarzer/Desktop/wikitest.xml",
                "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath()
        });
    }

    @Test
    public void RedirectionExecution() throws Exception {

        RedirectExtractor.main(new String[]{
                "file://" + getClass().getClassLoader().getResources("wikiRedirect.xml").nextElement().getPath(),
//               "print" //outputFilename
                "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath()

        });
    }

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
    public void DoubleListPerformance() {
        int runs = 999999;
        String testStr = "1.0|2.0|100|0.05|1245.67";
        String delimiter = Pattern.quote("|");

        long start = System.nanoTime();
        for (int i = 0; i < runs; i++) {
            DoubleListValue.valueOf(testStr, delimiter);
        }
        long time = System.nanoTime() - start;
        System.out.printf("Parse DoubleListValue took an average of %.1f us%n", time / runs / 1000.0);

        long startB = System.nanoTime();
        for (int i = 0; i < runs; i++) {
            StringUtils.getDoubleListFromString(testStr, delimiter);
        }
        long timeB = System.nanoTime() - startB;
        System.out.printf("Parse ArrayList<Double> took an average of %.1f us%n", timeB / runs / 1000.0);

    }

    @Test
    public void encodeTest() {
        String s = "Usinger|Usinger&#039;s"
                + "\nChūbu"
                + "\n中部地方"
                + "\n&amp;";

        System.out.println(" A = " + StringUtils.unescapeEntities(s));
        System.out.println(" B = " + StringEscapeUtils.unescapeHtml4(s));
    }


}