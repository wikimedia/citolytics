package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.evaluation.CPAResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.Evaluation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import java.io.IOException;

public class EvaluationTest {
    @Test
    public void TestLocal() throws Exception {
        String inputWikiFilename = "file://" + getClass().getClassLoader().getResources("evaluation_wikisim.csv").nextElement().getPath();
        String inputSeeAlsoFilename = "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath();

        String outputFilename = "file://" + getClass().getClassLoader().getResources("evaluation.out").nextElement().getPath();

        Evaluation.main(new String[]{inputSeeAlsoFilename, inputWikiFilename, outputFilename});
    }

    @Test
    public void TestCSVInput() throws Exception {
        String inputWikiFilename = "file://" + getClass().getClassLoader().getResources("testresult.csv").nextElement().getPath();

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<CPAResult> res = env.readCsvFile(inputWikiFilename)
                .fieldDelimiter('|')

                .includeFields("0110001000")
                .tupleType(CPAResult.class);

        res.print();

        env.execute("CSV Input test");

    }

    @Test
    public void TestCSVInput2() throws Exception {
        String inputCsvFilename = "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath();

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<Tuple2<String, String>> res = env.readCsvFile(inputCsvFilename)
                .fieldDelimiter('|')

                        //.includeFields("0110001000")
                .types(String.class, String.class);

        res.print();

        env.execute("CSV Input test");

    }
}
