package de.tuberlin.dima.schubotz.wikisim.cpa.tests;

import de.tuberlin.dima.schubotz.wikisim.clickstream.ValidateClickStreamData;
import de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils.Tester;
import org.junit.Ignore;
import org.junit.Test;

public class ClickStreamTest extends Tester {
    @Ignore
    @Test
    public void validateLocalDataSet() throws Exception {
        ValidateClickStreamData.main(
                new String[]{
                        resource("2015_02_clickstream_preview.tsv"),
                        "print"
                });
    }

    @Test
    public void validateDataSet() throws Exception {
        ValidateClickStreamData job = new ValidateClickStreamData();

        job.start(new String[]{resource("2015_02_clickstream_preview.tsv"), "local"});

        if (job.output.size() != 44)
            throw new Exception("Number of results != 44");
    }
}
