package org.wikipedia.citolytics.tests;

import org.junit.Before;
import org.junit.Ignore;
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

    @Before
    public void before() throws FileNotFoundException {
        wikiSimPath = resource("wikisim_output.csv");
    }


    @Ignore
    @Test
    public void TestPrepareOutputPrint() throws Exception {
        PrepareOutput.main(("--wikisim " + wikiSimPath
                + " --wikidump " + resource("wikiSeeAlso2.xml") + " --output print --topk 10").split(" "));
    }

    @Ignore
    @Test
    public void testElasticBulkOutputIgnoreIds() throws Exception {
        PrepareOutput.main(("--wikisim " + resource("wikisim_output.csv")
                + " --wikidump " + resource("wikiSeeAlso2.xml")
                + " --enable-elastic --ignore-missing-ids --output print --topk 10").split(" "));
    }


    @Ignore
    @Test
    public void testElasticBulkOutput() throws Exception {
        PrepareOutput.main(("--wikidump " + resource("wikisim_missingids.xml")
                + " --enable-elastic --output print --topk 10").split(" "));
    }

    @Ignore
    @Test
    public void TestPrepareOutputDisabledScores() throws Exception {
        PrepareOutput.main(("--wikisim " + resource("wikisim_output.csv") + " --disable-scores --output print --topk 10").split(" "));
    }

    @Ignore
    @Test
    public void TestPrepareOutputSave() throws Exception {
        PrepareOutput.main(("--wikisim " + resource("wikisim_output.csv") + " --output " + resource("cirrussearch.json")).split(" "));
    }

    @Ignore
    @Test
    public void TestIdTitleMappingExtractor() throws Exception {
        IdTitleMappingExtractor.main(("--input " + resource("wikiSeeAlso2.xml") + " --output print").split(" "));
    }
}
