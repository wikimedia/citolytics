package de.tuberlin.dima.schubotz.wikisim.cpa.tests;

import de.tuberlin.dima.schubotz.wikisim.cpa.TestOutput;
import de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim;
import de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils.TestUtils;
import de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils.Tester;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.LinkTuple;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.wikisim.histogram.Histogram;
import de.tuberlin.dima.schubotz.wikisim.linkgraph.LinksExtractor;
import de.tuberlin.dima.schubotz.wikisim.redirects.RedirectExtractor;
import de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoEvaluation;
import de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoExtractor;
import org.junit.Ignore;
import org.junit.Test;

public class CalculationTest extends Tester {

    /**
     * Tests complete functionality of WikiSim application.
     * <p/>
     * Input:
     * a) Wikipedia XML Dump (including SeeAlso links, redirects, ..)
     * b) ClickStream DataSet
     * <p/>
     * Prepare:
     * 1. Extract redirects
     * 2. WikiSim with redirects
     * 3. Extract SeeAlso links
     * 4. Resolve redirects in SeeAlso links
     * Evaluation:
     * 5. SeeAlso
     * 6. ClickStreams
     * ====
     * Test evaluation results
     *
     * @throws Exception
     */
    @Test
    public void CompleteTest() throws Exception {
        // TODO

        String inputA = resource("completeTestWikiDump.xml");
        String inputB = resource("completeTestClickStreamDataSet.tsv");

        RedirectExtractor job1 = new RedirectExtractor();
        job1.start(new String[]{
                inputA,
                resource("completeTestRedirects.out")
        });

        WikiSim job2 = new WikiSim();
        job2.start(new String[]{
                inputA,
                resource("completeTestWikiSim.out"),
                "2", "0", "0", "n",
                resource("completeTestRedirects.out")
        });

        SeeAlsoExtractor job3 = new SeeAlsoExtractor();
        job3.start(new String[]{
                inputA,
                resource("completeTestSeeAlso.out")
        });

//        SeeAlsoRedirects job4 = new SeeAlsoRedirects();
//        job4.start(new String[]{
//                resource("completeTestSeeAlso.out"),
//                resource("completeTestRedirects.out"),
//                resource("completeTestSeeAlso.out")
//        });

        SeeAlsoEvaluation job5 = new SeeAlsoEvaluation();
        job5.start(new String[]{
                resource("completeTestWikiSim.out"),
                "local",
                resource("completeTestSeeAlso.out")
        });


        System.out.println(job5.output);


    }

    @Ignore
    @Test
    public void TestLocalExecution() throws Exception {

        WikiSim.main(new String[]{
                resource("wikiSeeAlso.xml"),
                "print",
                "1.5,1.25,1,0.5,0",
                "1"});
    }

    @Test
    public void TestResultCount() throws Exception {
        WikiSim job = new WikiSim();

        job.start(new String[]{
                "file://" + getClass().getClassLoader().getResources("wikiSeeAlso.xml").nextElement().getPath(),
                "local"
        });

        // if == 34202
        System.out.println(job.output.size());

    }

    @Ignore
    @Test
    public void ValidateWikiSimOutput() throws Exception {

        TestOutput.main(new String[]{
                resource("testresult2.csv"),
                "print"
        });
    }


    @Ignore
    @Test
    public void TestWiki2006() throws Exception {

        String inputFilename = "file://" + getClass().getClassLoader().getResources("wiki2006.xml").nextElement().getPath();
        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.out").nextElement().getPath();

        outputFilename = "print";

        WikiSim.main(new String[]{inputFilename, outputFilename, "0.81,1.5,1.25", "0", "0"});
    }

    @Ignore
    @Test
    public void TestRedirectedExecution() throws Exception {

        String inputFilename = "file://" + getClass().getClassLoader().getResources("wikiRedirectedLinks.xml").nextElement().getPath();

//        inputFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso.xml").nextElement().getPath();


        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.out").nextElement().getPath();
//        String outputFilename = "print";

        WikiSim.main(new String[]{inputFilename, outputFilename, "1.5,1.75", "0", "0", "n", "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath()});
    }

    @Ignore
    @Test
    public void TestHistogram() throws Exception {

        //  wikiTalkPage.xml"
        String inputFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso2.xml").nextElement().getPath();
        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.out").nextElement().getPath();


        outputFilename = "print";

        Histogram.main(new String[]{inputFilename, outputFilename});
    }


    @Ignore
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
