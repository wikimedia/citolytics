package org.wikipedia.citolytics.tests;

import org.apache.flink.api.java.LocalEnvironment;
import org.junit.Test;
import org.wikipedia.citolytics.cirrussearch.IdTitleMappingExtractor;
import org.wikipedia.citolytics.cpa.types.IdTitleMapping;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class IdTitleMappingExtractorTest extends Tester {

    @Test
    public void testIdTitleMappingExtractor() throws Exception {
        IdTitleMappingExtractor job = new IdTitleMappingExtractor();

        job.enableTestEnvironment()
                .start("--input " + resource("wiki.xml", true)+ " --output print");
    }

    @Test
    public void testExtractIdTitleMappingToFile() throws Exception {
        IdTitleMappingExtractor job = new IdTitleMappingExtractor();
        job.enableTestEnvironment().start("--input " + resource("wiki.xml", true)
                + " --output " + resource("idtitle_mapping.out", true));
    }

    @Test
    public void testExtractAndReadIdTitleMapping() throws Exception {
        IdTitleMappingExtractor job = new IdTitleMappingExtractor();
        job.enableTestEnvironment().start("--input " + resource("wiki.xml", true)
                + " --output local");

        LocalEnvironment env = new LocalEnvironment();
        List<IdTitleMapping> expected = IdTitleMappingExtractor.getIdTitleMapping(env,
                resource("idtitle_mapping.in", true), null).collect();

//        System.out.println(job.output);
//        System.out.println(expected);

        assertTrue("Outputs should be the same", job.output.containsAll(expected) && expected.containsAll(job.output));
    }

}