package org.wikipedia.citolytics.tests;

import org.junit.Test;
import org.wikipedia.citolytics.cpa.WikiSim;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

/**
 * @author malteschwarzer
 */
public class WikiSimTest extends Tester {
    public void assertOutput(List<WikiSimResult> actual, String pathToExpected) throws Exception {
        HashSet<WikiSimResult> expected = new HashSet<>();

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(pathToExpected).getFile());

        try (Scanner scanner = new Scanner(file)) {

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                expected.add(WikiSimResult.valueOf(line, "|"));
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
        for (WikiSimResult a : actual) {

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

    @Test
    public void testSimple() throws Exception {
        WikiSim job = new WikiSim();

        job.verbose().start(("--input " + resource("fixtures/wikisim_simple.xml.input") + " --output local").split(" "));
        assertOutput(job.getOutput(), "fixtures/wikisim_simple.xml.expected");

//        job.enableSingleOutputFile()
//                .verbose()
//                .start(("--input " + resource("fixtures/wikisim_simple.xml.input") + " --group-reduce --output " + resource("fixtures/wikisim_simple.xml.expected")).split(" "));


    }
}
