package de.tuberlin.dima.schubotz.wikisim.cpa.tests;

import de.tuberlin.dima.schubotz.wikisim.clickstream.ClickStreamEvaluation;
import de.tuberlin.dima.schubotz.wikisim.clickstream.types.ClickStreamResult;
import de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim;
import de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils.TestUtils;
import de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils.Tester;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.LinkTuple;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.wikisim.cpa.utils.CheckOutputIntegrity;
import de.tuberlin.dima.schubotz.wikisim.cpa.utils.ValidateOrderInOutput;
import de.tuberlin.dima.schubotz.wikisim.histogram.Histogram;
import de.tuberlin.dima.schubotz.wikisim.linkgraph.LinksExtractor;
import de.tuberlin.dima.schubotz.wikisim.redirects.RedirectExtractor;
import de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoEvaluation;
import de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoExtractor;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.SeeAlsoEvaluationResult;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
     * (4. Resolve redirects in SeeAlso links)
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
        job1.enableSingleOutputFile();
        job1.start(new String[]{
                inputA,
                resource("completeTestRedirects.out")
        });

        WikiSim job2 = new WikiSim();
        job2.enableSingleOutputFile();
        job2.start(new String[]{
                inputA,
                resource("completeTestWikiSim.out"),
                "2", "0", "0", "n",
                resource("completeTestRedirects.out")
        });

        SeeAlsoExtractor job3 = new SeeAlsoExtractor();
        job3.enableSingleOutputFile();
        job3.start(new String[]{
                inputA,
                resource("completeTestSeeAlso.out"),
                resource("completeTestRedirects.out") // job4
        });

        SeeAlsoEvaluation job5 = new SeeAlsoEvaluation();
        job5.start(new String[]{
                resource("completeTestWikiSim.out"),
                "local",
                resource("completeTestSeeAlso.out")
        });

        ClickStreamEvaluation job6 = new ClickStreamEvaluation();
        job6.start(new String[]{
                resource("completeTestWikiSim.out"),
                inputB,
                "local"
        });

        // Test SeeAlso evaluation
        int found = 0;

        for (SeeAlsoEvaluationResult r : job5.output) {
            if (r.f0.equals("SeeAlso Article 1")) {
                if (r.f4 != 6 || r.f5 != 1.0 || r.f6 != 0.5) {
                    throw new Exception("Invalid scores for SeeAlso Article 1");
                }
                found++;
            }

            if (r.f0.equals("SeeAlso Article 2")) {

                if (r.f4 != 6 || r.f5 != 1.0 || r.f6 != 0.5 || r.f7 != 0.45 || r.f8 != 2) {
                    throw new Exception("Invalid scores for SeeAlso Article 2");
                }
                found++;
            }

        }

        if (found != 2) {
            throw new Exception("SeeAlso evaluation output size is invalid.");
        }

        // Test ClickStream
        found = 0;

        for (ClickStreamResult r : job6.output) {
            if (r.f0.equals("Article C")) {
                if (r.f2 != 6 || r.f3 != 99 || r.f4 != 0 || r.f5 != 0 || r.f6 != 0 || r.f7 != 0) {
                    throw new Exception("Invalid clicks for Article C");
                }
                found++;
            }


            if (r.f0.equals("Article A")) {
                if (r.f2 != 6 || r.f3 != 0 || r.f4 != 20 || r.f5 != 20 || r.f6 != 20 || r.f7 != 20) {
                    throw new Exception("Invalid clicks for Article C");
                }
                found++;
            }
        }

        if (found != 2) {
            throw new Exception("ClickStream evaluation output is invalid.");
        }

//        System.out.println(job6.output);


//        System.out.println(job5.output);


    }

    @Test
    public void TestResultCount() throws Exception {
        WikiSim job = new WikiSim();

        job.start(new String[]{
                resource("wikiSeeAlso.xml"),
                "local"
        });

        // If threshold is greater than 0, result count varies
        assertEquals("WikiSim result count is wrong", 126253, job.output.size());

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

    @Ignore
    @Test
    public void ValidateWikiSimOutput() throws Exception {

        ValidateOrderInOutput.main(new String[]{
                resource("wikisim_output.csv"),
                "print"
        });
    }

    @Test
    public void ValidateWikiSimOutputIntegrity() throws Exception {

        CheckOutputIntegrity.main(new String[]{
                resource("wikisim_output.csv"),
                resource("wikisim_output_b.csv"),
                "print"
        });
    }


    @Ignore
    @Test
    public void TestWiki2006() throws Exception {

        WikiSim job = new WikiSim();
        job.start(new String[]{
                resource("wiki2006.xml"),
                "local", "0.81,1.5,1.25", "0", "0", "2006"});

        assertEquals("Result count wrong", 87632, job.output.size());
    }

    @Test
    public void TestRedirectedExecution() throws Exception {

        WikiSim job = new WikiSim();

        job.start(new String[]{
                resource("wikiRedirectedLinks.xml"),
                "local",
                "1.5,1.75", "0", "0", "n",
                resource("redirects.out")
        });

        assertEquals("Result count wrong", 3, job.output.size());

        List<WikiSimResult> needles = new ArrayList<>();

        needles.add(new WikiSimResult("Foo", "Redirect target", 17, 2, new double[]{0.08123121086119625, 0.047661356279998054}));
        needles.add(new WikiSimResult("Bar", "Foo", 9, 2, new double[]{0.40754831530887764, 0.33049721878532895}));
        needles.add(new WikiSimResult("Bar", "Redirect target", 8, 2, new double[]{0.4215947723372509, 0.3407763504193827}));

        assertTrue("Needles not found.", job.output.containsAll(needles));

//        System.out.println(job.output);
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

    @Ignore
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
