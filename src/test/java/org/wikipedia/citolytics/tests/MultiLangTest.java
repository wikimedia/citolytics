package org.wikipedia.citolytics.tests;

import org.junit.Test;
import org.wikipedia.citolytics.multilang.MultiLang;
import org.wikipedia.citolytics.tests.utils.Tester;

public class MultiLangTest extends Tester {
    @Test
    public void plan() throws Exception {
        MultiLang job = new MultiLang();

        job.start("--input " + resource("langlinks.sql") + " --output local");

        System.out.println(job.output.size());


    }

}