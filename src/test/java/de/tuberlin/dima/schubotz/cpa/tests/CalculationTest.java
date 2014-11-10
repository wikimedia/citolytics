package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.WikiSim;
import org.junit.Test;

/**
 * Created by malteschwarzer on 15.10.14.
 */
public class CalculationTest {
    @Test
    public void TestLocalExecution() throws Exception {

        String inputFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso.xml").nextElement().getPath();
        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.csv").nextElement().getPath();

        WikiSim.main(new String[]{inputFilename, outputFilename, "1.5", "0"});
    }
}
