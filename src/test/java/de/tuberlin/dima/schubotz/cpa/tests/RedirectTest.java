package de.tuberlin.dima.schubotz.cpa.tests;


import de.tuberlin.dima.schubotz.cpa.redirects.SeeAlsoRedirects;
import de.tuberlin.dima.schubotz.cpa.redirects.WikiSimRedirects;
import de.tuberlin.dima.schubotz.cpa.types.list.DoubleListValue;
import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
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


}
