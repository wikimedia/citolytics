package org.wikipedia.citolytics.tests;

import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.cirrussearch.PrepareOutput;
import org.wikipedia.citolytics.tests.utils.Tester;

/**
 * @author malteschwarzer
 */
public class CirrussearchTest extends Tester {

    @Ignore
    @Test
    public void TestPrepareOutputPrint() throws Exception {
        PrepareOutput.main(("--input " + resource("wikisim_output.csv") + " --output print --topk 10").split(" "));
    }


    @Ignore
    @Test
    public void TestPrepareOutputDisabledScores() throws Exception {
        PrepareOutput.main(("--input " + resource("wikisim_output.csv") + " --disable-scores --output print --topk 10").split(" "));
    }

    @Ignore
    @Test
    public void TestPrepareOutputSave() throws Exception {
        PrepareOutput.main(("--input " + resource("wikisim_output.csv") + " --output " + resource("cirrussearch.json")).split(" "));
    }

}
