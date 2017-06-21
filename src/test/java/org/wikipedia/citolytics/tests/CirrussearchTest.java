package org.wikipedia.citolytics.tests;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.cirrussearch.PrepareOutput;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;

/**
 * Test for all CirrusSearch-related jobs
 */
public class CirrussearchTest extends Tester {
    private String wikiSimPath;
    private String wikiDumpPath;
    private String outputPath;
    private String missingIdsPath;
    private String articleStatsPath;


    @Before
    public void before() throws FileNotFoundException {
        wikiSimPath = resource("wikisim_output.csv", true);
        wikiDumpPath = resource("wikiSeeAlso2.xml",true);
        outputPath = resource("citolytics.json.out",true);
        missingIdsPath = resource("missing_ids.xml",true);
        articleStatsPath = resource("stats.in", true);
    }

    @Ignore
    @Test
    public void testPrintOutput() throws Exception {
        PrepareOutput job = new PrepareOutput();

        job.enableTestEnvironment()
                .start("--wikidump " + wikiDumpPath + " --output print --topk 10");
    }

    @Ignore
    @Test
    public void testElasticBulkOutputIgnoreIds() throws Exception {
        fixture = "elastic_bulk_ignore_ids.xml";

        PrepareOutput job = new PrepareOutput();

        job.enableTestEnvironment()
                .start("--wikidump " + getInputPath()
                + " --enable-elastic --ignore-missing-ids --output local --topk 10");

        assertJobOutputStringWithFixture(job, "Invalid output");
    }

    @Test
    public void testElasticBulkOutput() throws Exception {
        PrepareOutput job = new PrepareOutput();

        job.enableTestEnvironment()
                .start("--wikidump " + missingIdsPath
                + " --enable-elastic --output local --topk 10");

        assertEquals("Invalid number of results", 3, job.getOutput().size());
    }

    @Test
    public void testPrepareOutputDisabledScores() throws Exception {
        PrepareOutput job = new PrepareOutput();

        job.enableTestEnvironment()
                .start("--wikisim " + wikiSimPath + " --disable-scores --output local --topk 10");

        assertEquals("Invalid number of results", 29, job.getOutput().size());
    }

    @Test
    public void testPrepareOutputSave() throws Exception {
        PrepareOutput job = new PrepareOutput();

        job.enableTestEnvironment()
                .start("--wikisim " + wikiSimPath + " --output " + outputPath);
    }


    @Test
    public void testCPI() throws Exception {
        fixture = "cpi_expression.xml";
        String cpiExpr = "x*log(z/(y+1))"; // x*log(z/(y+1))
        PrepareOutput job = new PrepareOutput();

        job.enableTestEnvironment()
                .start("--wikidump " + getInputPath()
                        + " --article-stats " + articleStatsPath
                        + " --cpi " + cpiExpr
                        + " --output local --ignore-missing-ids --topk 3");

        assertJobOutputStringWithFixture(job, "Invalid output");
    }

    @Test
    public void testElasticBulkOutputIgnoreIdsBackupRecs() throws Exception {
        fixture = "backup_recommendations.xml";
        PrepareOutput job = new PrepareOutput();

        // --enable-elastic
        // --backup-recommendations
        job.enableTestEnvironment()
                .start("--wikidump " + getInputPath()
                        + " --ignore-missing-ids --output local --topk 10"
                        + " --backup-recommendations");
        assertJobOutputStringWithFixture(job, "Invalid output");

        //        assertEquals("Invalid response", FileUtils.readFileToString(new File(getExpectedOutputPath()), String.join("\n", job.getOutput())));

    }
}
