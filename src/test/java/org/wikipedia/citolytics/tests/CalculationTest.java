package org.wikipedia.citolytics.tests;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.clickstream.ClickStreamEvaluation;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.cpa.WikiSim;
import org.wikipedia.citolytics.cpa.types.LinkTuple;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;
import org.wikipedia.citolytics.cpa.utils.CheckOutputIntegrity;
import org.wikipedia.citolytics.cpa.utils.ValidateOrderInOutput;
import org.wikipedia.citolytics.histogram.Histogram;
import org.wikipedia.citolytics.linkgraph.LinksExtractor;
import org.wikipedia.citolytics.redirects.RedirectExtractor;
import org.wikipedia.citolytics.seealso.SeeAlsoEvaluation;
import org.wikipedia.citolytics.seealso.SeeAlsoExtractor;
import org.wikipedia.citolytics.seealso.types.SeeAlsoEvaluationResult;
import org.wikipedia.citolytics.tests.utils.TestUtils;
import org.wikipedia.citolytics.tests.utils.Tester;

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
        job1.enableSingleOutputFile()
                .verbose()
                .start(new String[]{
                        inputA,
                        resource("completeTestRedirects.out")
                });

        WikiSim job2 = new WikiSim();
        job2.enableSingleOutputFile()
                .verbose()
                .start(new String[]{
                        inputA,
                        resource("completeTestWikiSim.out"),
                        "2", "0", "0", "n",
                        resource("completeTestRedirects.out")
                });

        SeeAlsoExtractor job3 = new SeeAlsoExtractor();
        job3.enableSingleOutputFile()
                .verbose()
                .start(new String[]{
                        inputA,
                        resource("completeTestSeeAlso.out"),
                        resource("completeTestRedirects.out") // job4
                });

        SeeAlsoEvaluation job5 = new SeeAlsoEvaluation();
        job5.verbose()
                .start(new String[]{
                        resource("completeTestWikiSim.out"),
                        "local",
                        resource("completeTestSeeAlso.out")
                });

        ClickStreamEvaluation job6 = new ClickStreamEvaluation();
        job6.verbose()
                .start(new String[]{
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

        job.verbose().start(("--input " + resource("wikiSeeAlso.xml") + " --output local").split(" "));

        // If threshold is greater than 0, result count varies
        Assert.assertEquals("WikiSim result count is wrong", 126253, job.output.size());

    }

    @Ignore
    @Test
    public void TestLocalExecution() throws Exception {

        WikiSim.main(("--input " + resource("wikiSeeAlso.xml") + " --output print --alpha 1.5,1.25,1,0.5,0").split(" "));
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

        CheckOutputIntegrity job = new CheckOutputIntegrity();

        job.verbose().start(new String[]{
                resource("wikisim_output.csv"),
                resource("wikisim_output_b.csv"),
                "print"
        });

//        System.out.println(job.output);
//        assertEquals("CheckOutputIntegrity should return errors.", 2, job.output.size());
    }

    @Ignore
    @Test
    public void NegativeAlphaCPI() throws Exception {
        WikiSim job = new WikiSim();
        job.start(new String[]{
                resource("completeTestWikiDump.xml"),
                "print",

                "1,-1"

        });
    }

    @Ignore
    @Test
    public void TestWiki2006() throws Exception {

        WikiSim job = new WikiSim();
        job.verbose().start(new String[]{
                resource("wiki2006.xml"),
                "local", "0.81,1.5,1.25", "0", "0", "2006"});

        Assert.assertEquals("Result count wrong", 87632, job.output.size());
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
        result.setCPI(new double[]{1.99, 10.99, 0.995, 1234.5678});

        System.out.println("String = " + TestUtils.sizeof(str));
        System.out.println("WikiSimResult = " + TestUtils.sizeof(result));

    }

    @Ignore
    @Test
    public void HashCollisionTest() throws Exception {
        // TODO Improve hash function.
        if (LinkTuple.getHash("NPR", "The Church of Jesus Christ of Latter-day Saints")
                == LinkTuple.getHash("Mp3", "The Church of Jesus Christ of Latter-day Saints")) {
            throw new Exception("Hashcodes are equal.");
        }


    }
}
