package org.wikipedia.citolytics.tests;

import org.junit.Test;
import org.wikipedia.citolytics.cirrussearch.IdTitleMappingExtractor;
import org.wikipedia.citolytics.cpa.types.IdTitleMapping;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IdTitleMappingExtractorTest extends Tester {

    @Test
    public void testIdTitleMappingExtractor() throws Exception {
        IdTitleMappingExtractor job = new IdTitleMappingExtractor();

        job.enableTestEnvironment()
                .start("--input " + resource("wiki.xml", true)+ " --output local");

        assertEquals("Invalid output size", 7, job.getOutput().size());

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

        List<IdTitleMapping> expected = IdTitleMappingExtractor.getIdTitleMapping(getSilentEnv(),
                resource("idtitle_mapping.in", true), null).collect();

//        System.out.println(job.output);
//        System.out.println(expected);

        assertTrue("Outputs should be the same", job.output.containsAll(expected) && expected.containsAll(job.output));
    }

}