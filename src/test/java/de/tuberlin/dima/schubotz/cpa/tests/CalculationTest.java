package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.RelationFinder;
import org.apache.flink.api.common.Plan;
import org.apache.flink.client.LocalExecutor;
import org.junit.Test;

/**
 * Created by malteschwarzer on 15.10.14.
 */
public class CalculationTest {
    @Test
    public void TestLocalExecution() throws Exception {


        RelationFinder rc = new RelationFinder();
        String inputFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso.xml").nextElement().getPath();
        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.csv").nextElement().getPath();

        System.out.println(inputFilename);
        System.out.println(outputFilename);

        Plan plan = rc.getPlan(inputFilename, outputFilename + Math.random() * Integer.MAX_VALUE, "1.5", "0");
        LocalExecutor.execute(plan);
    }
}
