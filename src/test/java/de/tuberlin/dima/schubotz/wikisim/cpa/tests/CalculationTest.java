package de.tuberlin.dima.schubotz.wikisim.cpa.tests;

import de.tuberlin.dima.schubotz.wikisim.cpa.TestOutput;
import de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim;
import de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils.TestUtils;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.LinkTuple;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.wikisim.histogram.Histogram;
import de.tuberlin.dima.schubotz.wikisim.linkgraph.LinksExtractor;
import org.junit.Test;

public class CalculationTest {
    @Test
    public void TestLocalExecution() throws Exception {

        String inputFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso.xml").nextElement().getPath();
        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.out").nextElement().getPath();
        outputFilename = "print";

        WikiSim.main(new String[]{inputFilename, outputFilename, "1.5,1.25,1,0.5,0", "1"});
    }

    @Test
    public void ValidateWikiSimOutput() throws Exception {

        String inputFilename = "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath();
        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.out").nextElement().getPath();
        outputFilename = "print";

        TestOutput.main(new String[]{inputFilename, outputFilename});
    }


    @Test
    public void TestWiki2006() throws Exception {

        String inputFilename = "file://" + getClass().getClassLoader().getResources("wiki2006.xml").nextElement().getPath();
        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.out").nextElement().getPath();

        outputFilename = "print";

        WikiSim.main(new String[]{inputFilename, outputFilename, "0.81,1.5,1.25", "0", "0"});
    }

    @Test
    public void TestRedirectedExecution() throws Exception {

        String inputFilename = "file://" + getClass().getClassLoader().getResources("wikiRedirectedLinks.xml").nextElement().getPath();

//        inputFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso.xml").nextElement().getPath();


        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.out").nextElement().getPath();
//        String outputFilename = "print";

        WikiSim.main(new String[]{inputFilename, outputFilename, "1.5,1.75", "0", "0", "n", "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath()});
    }

    @Test
    public void TestHistogram() throws Exception {

        //  wikiTalkPage.xml"
        String inputFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso2.xml").nextElement().getPath();
        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.out").nextElement().getPath();


        outputFilename = "print";

        Histogram.main(new String[]{inputFilename, outputFilename});
    }


    @Test
    public void TestLinkExtractor() throws Exception {

        String inputFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso2.xml").nextElement().getPath();
        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.out").nextElement().getPath();

        LinksExtractor.main(new String[]{inputFilename, outputFilename});
    }

    @Test
    public void IntermediateResultSize() throws Exception {
        String str = "ABC";
        WikiSimResult result = new WikiSimResult(new LinkTuple("Page AAAAA", "Page BBBB"), 999);

        result.setDistSquared(9999);
        result.setCPA(new double[]{1.99, 10.99, 0.995, 1234.5678});

        System.out.println("String = " + TestUtils.sizeof(str));
        System.out.println("WikiSimResult = " + TestUtils.sizeof(result));

    }
}
