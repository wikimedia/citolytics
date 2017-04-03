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
    public void testExtractIdTitleMappingToFile() throws Exception {
        IdTitleMappingExtractor job = new IdTitleMappingExtractor();
        job.enableSingleOutputFile().start("--input " + resource("IdTitleMappingExtractorTest/wiki.xml")
                + " --output " + resource("IdTitleMappingExtractorTest/idtitle_mapping.out"));
    }

    @Test
    public void testExtractAndReadIdTitleMapping() throws Exception {
        IdTitleMappingExtractor job = new IdTitleMappingExtractor();
        job.start("--input " + resource("IdTitleMappingExtractorTest/wiki.xml")
                + " --output local");

        LocalEnvironment env = new LocalEnvironment();
        List<IdTitleMapping> expected = IdTitleMappingExtractor.getIdTitleMapping(env,
                resource("IdTitleMappingExtractorTest/idtitle_mapping.in"), null).collect();

//        System.out.println(job.output);
//        System.out.println(expected);

        assertTrue("Outputs should be the same", job.output.containsAll(expected) && expected.containsAll(job.output));
    }

}