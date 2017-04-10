package org.wikipedia.citolytics.tests;

import org.junit.Before;
import org.junit.Test;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.WikiSim;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.utils.ValidateOrderInOutput;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;

public class WikiSimTest extends Tester {
    private String fixture;
    private WikiSimAbstractJob job;

    @Before
    public void setUp() throws Exception {
        job = new WikiSim();
        job.silent();
        job.enableLocalEnvironment();
    }

    public void assertOutput(List<RecommendationPair> actual, String pathToExpected) throws Exception {
        HashSet<RecommendationPair> expected = new HashSet<>();

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(pathToExpected).getFile());

        try (Scanner scanner = new Scanner(file)) {

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                expected.add(RecommendationPair.valueOf(line, "|"));
            }

            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Test sizes
        if (expected.size() != actual.size()) {
            throw new Exception("Invalid number of actual output records. Expected: " + expected.size() +
                    ". Actual: " + actual.size());
        }

        // Test every records
        int correct = 0;
        for (RecommendationPair a : actual) {

//            System.out.println(a);
            if (expected.contains(a)) {
                correct++;
            }
        }

        if (correct != expected.size()) {
            throw new Exception("Invalid number of correct output records. Expected: " + expected.size() +
                    ". Actual: " + correct + " / " + actual.size());
        }
    }

    private String getInputPath() throws FileNotFoundException {
        return resource("WikiSimTest/" + fixture + ".input");
    }

    private String getExpectedOutputPath() {
        return "WikiSimTest/" + fixture + ".expected";
    }


    @Test
    public void testSimple() throws Exception {
        fixture = "simple.xml";

        job.start("--input " + getInputPath() + " --output local");

        assertOutput(job.getOutput(), getExpectedOutputPath());

//        job.enableSingleOutputFile()
//                .silent()
//                .start(("--input " + resource("WikiSimTest/simple.xml.input") + " --group-reduce --output " + resource("WikiSimTest/simple.xml.expected")).split(" "));


    }

    @Test
    public void testRelativeProximity() throws Exception {
        fixture = "relative_proximity.xml";
        job.start("--input " + getInputPath() + " --relative-proximity --output local");

        assertOutput(job.getOutput(), getExpectedOutputPath());

    }

    @Test
    public void testAlpha() throws Exception {
        fixture = "alpha.xml";

        job.start("--input " + getInputPath() + " --alpha 0.5,0.9,2.0,-1.0 --output local");

        assertOutput(job.getOutput(), getExpectedOutputPath());
    }

    @Test
    public void testSimpleWithRedirects() throws Exception {
        fixture = "simple_with_redirects.xml";


    }


    @Test
    public void testValidateWikiSimOutput() throws Exception {
        setJob(new ValidateOrderInOutput())
                .start("--input " + resource("wikisim.in", true) + " --output local");

        assertEquals("Invalid result count", 0, job.output.size());
    }

}