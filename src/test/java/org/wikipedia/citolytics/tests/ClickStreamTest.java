package org.wikipedia.citolytics.tests;

import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.clickstream.ClickStreamEvaluation;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.clickstream.utils.ValidateClickStreamData;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

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

        job.verbose().start(new String[]{resource("2015_02_clickstream_preview.tsv"), "local"});

        if (job.output.size() != 44)
            throw new Exception("Number of results != 44");
    }

    @Ignore
    @Test
    public void TestClickStreamEvaluation() throws Exception {
        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.start("--wikisim " + resource("wikisim_output.csv") // TODO check resource conflict with other tests
                + " --gold " + resource("2015_02_clickstream_preview.tsv")
                + " --output local");

        // Needles
        ArrayList<ClickStreamResult> needles = new ArrayList<>();

        ClickStreamResult n1 = new ClickStreamResult();
        n1.f0 = "QQQ";
        n1.f1 = new ArrayList<>();
        n1.f1.add(new Tuple3<>("CPA link", 14.0, 10));
        n1.f1.add(new Tuple3<>("CPA nolink", 14.0, 20));
        n1.f2 = 2;
        n1.f3 = 0;
        n1.f4 = 38;
        n1.f5 = 30;
        n1.f6 = 30;
        n1.f7 = 10;

        needles.add(n1);

        System.out.println(job.output);

        assertTrue("Needles not found", job.output.containsAll(needles));
    }
}
