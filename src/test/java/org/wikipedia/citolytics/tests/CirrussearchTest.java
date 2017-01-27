package org.wikipedia.citolytics.tests;

import org.apache.flink.api.java.tuple.Tuple1;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cirrussearch.IdTitleMappingExtractor;
import org.wikipedia.citolytics.cirrussearch.PrepareOutput;
import org.wikipedia.citolytics.tests.utils.Tester;

/**
 * @author malteschwarzer
 */
public class CirrussearchTest extends Tester {

    @Ignore
    @Test
    public void TestPrepareOutputPrint() throws Exception {
        PrepareOutput.main(("--wikisim " + resource("wikisim_output.csv")
                + " --wikidump " + resource("wikiSeeAlso2.xml") + " --output print --topk 10").split(" "));
    }

    @Ignore
    @Test
    public void testElasticBulkOutput() throws Exception {
        PrepareOutput.main(("--wikisim " + resource("wikisim_output.csv")
                + " --wikidump " + resource("wikiSeeAlso2.xml")
                + " --enable-elastic --ignore-missing-ids --output print --topk 10").split(" "));
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
