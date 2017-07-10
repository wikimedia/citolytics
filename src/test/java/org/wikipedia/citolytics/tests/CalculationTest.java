package org.wikipedia.citolytics.tests;

import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.clickstream.ClickStreamEvaluation;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.cpa.WikiSim;
import org.wikipedia.citolytics.cpa.types.LinkPair;
import org.wikipedia.citolytics.cpa.utils.CheckOutputIntegrity;
import org.wikipedia.citolytics.redirects.RedirectExtractor;
import org.wikipedia.citolytics.seealso.SeeAlsoEvaluation;
import org.wikipedia.citolytics.seealso.SeeAlsoExtractor;
import org.wikipedia.citolytics.seealso.types.SeeAlsoEvaluationResult;
import org.wikipedia.citolytics.tests.utils.Tester;

import static org.junit.Assert.assertEquals;

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
    @Ignore // TODO Separte this test to avoid memory kills
    public void CompleteTest() throws Exception {
        // TODO

        String inputA = resource("ArticleStatsTest/completeTestWikiDump.xml");
        String inputB = resource("completeTestClickStreamDataSet.tsv");

        RedirectExtractor job1 = new RedirectExtractor();
        job1.enableTestEnvironment()
                .start("--input " + inputA + " --output " + resource("completeTestRedirects.out"));

        WikiSim job2 = new WikiSim();
        job2.enableTestEnvironment()
                .start("--input " + inputA + " --output " + resource("completeTestWikiSim.out") + "" +
                        " --alpha 2 --redirects " + resource("completeTestRedirects.out"));

        SeeAlsoExtractor job3 = new SeeAlsoExtractor();
        job3.enableTestEnvironment()
                .start("--input " + inputA
                        + " --output " + resource("completeTestSeeAlso.out")
                        + " --redirects " + resource("completeTestRedirects.out"));

        SeeAlsoEvaluation job5 = new SeeAlsoEvaluation();
        job5.enableTestEnvironment()
                .start("--wikisim " + resource("completeTestWikiSim.out")
                        + " --output local"
                        + " --gold " + resource("completeTestSeeAlso.out"));

        ClickStreamEvaluation job6 = new ClickStreamEvaluation();
        job6.enableTestEnvironment()
                .start("--wikisim " + resource("completeTestWikiSim.out")
                        + " --gold " + inputB
                        + " --output local");

        // Test SeeAlso evaluation
//        System.out.println(job5.output);

        for (SeeAlsoEvaluationResult r : job5.output) {
            if (r.getArticle().equals("SeeAlso Article 1")) {
                assertEquals("Invalid getRetrievedDocsCount", 6, r.getRetrievedDocsCount());
                assertEquals("Invalid getHRR", 1, r.getHRR(), 0e-10);
                assertEquals("Invalid getTopKScore", 0.5, r.getTopKScore(), 0e-10);
            }

            if (r.getArticle().equals("SeeAlso Article 2")) {
                assertEquals("Invalid getRetrievedDocsCount", 6, r.getRetrievedDocsCount());
                assertEquals("Invalid getHRR", 1, r.getHRR(), 0e-10);
                assertEquals("Invalid getTopKScore", 0.5, r.getTopKScore(), 0e-10);
                assertEquals("Invalid getPerformanceMeasure", 0.45, r.getPerformanceMeasure(), 0e-10);
                assertEquals("Invalid getRelevantCount1", 2, r.getRelevantCount1());
            }

        }
        assertEquals("SeeAlso evaluation output size is invalid.", 2, job5.getOutput().size());


        // Test ClickStream
        int found = 0;

        for (ClickStreamResult r : job6.output) {
            if (r.getArticle().equals("Article C")) {
                assertEquals("Invalid getResultsCount (result=" + r + ")", 7, r.getRecommendations().size());
                assertEquals("Invalid getImpressions (result=" + r + ")", 99, r.getImpressions());
                assertEquals("Invalid getTotalClicks (result=" + r + ")", 0, r.getTotalClicks());
                assertEquals("Invalid getClicks1", 0, r.getClicks1());
                assertEquals("Invalid getClicks2", 0, r.getClicks2());
                assertEquals("Invalid getClicks3", 0, r.getClicks3());

                found++;
            }


            if (r.getArticle().equals("Article A")) {
                assertEquals("Invalid getResultsCount (result=" + r + ")", 6, r.getRecommendations().size());
                assertEquals("Invalid getImpressions (result=" + r + ")", 0, r.getImpressions());
                assertEquals("Invalid getTotalClicks (result=" + r + ")", 20, r.getTotalClicks());
                assertEquals("Invalid getClicks1", 20, r.getClicks1());
                assertEquals("Invalid getClicks2", 20, r.getClicks2());
                assertEquals("Invalid getClicks3", 20, r.getClicks3());

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
    public void TestResultCountKeepInfoBox() throws Exception {
        WikiSim job = new WikiSim();

        // If threshold is greater than 0, result count varies
        // without infobox removal= 126253
        // with infobox removal= 118341
        // with old namespace check = 126253
        job.enableTestEnvironment()
                .start(("--input " + resource("ArticleStatsTest/wikiSeeAlso.xml") + " --keep-infobox --output local").split(" "));

        assertEquals("WikiSim result count is wrong (keep infobox)", 125751, job.output.size());
    }

    @Test
    public void testResultsCountRemovedInfoBox() throws Exception {
        // with old namespace check = 118341
        WikiSim job = new WikiSim();
        job.enableTestEnvironment()
                .start(("--input " + resource("ArticleStatsTest/wikiSeeAlso.xml") + " --output local").split(" "));

        assertEquals("WikiSim result count is wrong (removed infobox)", 117855, job.output.size());
    }


    @Ignore
    @Test
    public void TestMissingIdRemoval() throws Exception {

        WikiSim job = new WikiSim();
        job
                .enableTestEnvironment()
                .start(("--input " + resource("wikisim_missingids.xml") + " --remove-missing-ids --output local").split(" "));

        assertEquals("Invalid output size", 3, job.output.size());

    }

    @Ignore
    @Test
    public void TestLocalExecution() throws Exception {

        WikiSim.main(("--input " + resource("ArticleStatsTest/wikiSeeAlso.xml") + " --output print --alpha 1.5,1.25,1,0.5,0").split(" "));
    }

    @Test
    public void ValidateWikiSimOutputIntegrity() throws Exception {

        CheckOutputIntegrity job = new CheckOutputIntegrity();

        job.enableTestEnvironment()
                .start(new String[]{
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
        job.start(("--input " + resource("ArticleStatsTest/completeTestWikiDump.xml") + " --output print --alpha 1,-1").split(" "));
    }

    @Ignore
    @Test
    public void TestWiki2006() throws Exception {

        WikiSim job = new WikiSim();
        job.enableLocalEnvironment()
                .silent()
                .start(new String[]{
                resource("wiki2006.xml"),
                "local", "0.81,1.5,1.25", "0", "0", "2006"});

        assertEquals("Result count wrong", 87632, job.output.size());
    }

    @Ignore
    @Test
    public void HashCollisionTest() throws Exception {
        // TODO Improve hash function.
        if (LinkPair.getHash("NPR", "The Church of Jesus Christ of Latter-day Saints")
                == LinkPair.getHash("Mp3", "The Church of Jesus Christ of Latter-day Saints")) {
            throw new Exception("Hashcodes are equal.");
        }


    }
}
