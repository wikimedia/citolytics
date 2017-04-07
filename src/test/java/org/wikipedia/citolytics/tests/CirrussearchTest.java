package org.wikipedia.citolytics.tests;

import org.junit.Before;
import org.junit.Test;
import org.wikipedia.citolytics.cirrussearch.IdTitleMappingExtractor;
import org.wikipedia.citolytics.cirrussearch.PrepareOutput;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.io.FileNotFoundException;

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

    @Test
    public void testPrintOutput() throws Exception {
        PrepareOutput.main(("--wikidump " + wikiDumpPath + " --output print --topk 10").split(" "));
    }

    @Test
    public void testElasticBulkOutputIgnoreIds() throws Exception {
        PrepareOutput.main(("--wikisim " + wikiSimPath
                + " --wikidump " + wikiDumpPath
                + " --enable-elastic --ignore-missing-ids --output print --topk 10").split(" "));
    }

    @Test
    public void testElasticBulkOutput() throws Exception {
        PrepareOutput.main(("--wikidump " + missingIdsPath
                + " --enable-elastic --output print --topk 10").split(" "));
    }

    @Test
    public void testPrepareOutputDisabledScores() throws Exception {
        PrepareOutput.main(("--wikisim " + wikiSimPath + " --disable-scores --output print --topk 10").split(" "));
    }

    @Test
    public void testPrepareOutputSave() throws Exception {
        PrepareOutput.main(("--wikisim " + wikiSimPath + " --output " + outputPath).split(" "));
    }

    @Test
    public void testIdTitleMappingExtractor() throws Exception {
        IdTitleMappingExtractor.main(("--input " + wikiDumpPath+ " --output print").split(" "));
    }

    @Test
    public void testCPI() throws Exception {
        String cpiExpr = "%1$f*Math.log(%3$d/%2$d)";
        PrepareOutput job = new PrepareOutput();

        job.enableLocalEnvironment()
                .start("--wikidump " + wikiDumpPath
                        + " --article-stats " + articleStatsPath
                        + " --cpi " + cpiExpr
                        + " --output print --topk 3");
    }
}
