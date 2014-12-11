package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.CPAResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.Evaluation;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.CPAResultInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

public class EvaluationTest {
    @Test
    public void TestLocal() throws Exception {
        String inputCpaFilename = "file://" + getClass().getClassLoader().getResources("evaluation_cpa.csv").nextElement().getPath();
        String inputSeeAlsoFilename = "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath();

        String outputFilename = "file://" + getClass().getClassLoader().getResources("evaluation.out").nextElement().getPath();

        outputFilename = "print";

        Evaluation.main(new String[]{inputSeeAlsoFilename, inputCpaFilename, outputFilename});
    }

    @Test
    public void FullEval() throws Exception {

        Evaluation.main(new String[]{
                "print",
                "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("testresult.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_mlt.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_links.csv").nextElement().getPath(),

                "20"
        });
    }

    @Test
    public void TestCSVInput() throws Exception {
        String inputWikiFilename = "file://" + getClass().getClassLoader().getResources("testresult.csv").nextElement().getPath();

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<CPAResult> res = env.readFile(new CPAResultInputFormat(), inputWikiFilename);
//
//        readCsvFile(inputWikiFilename)
//                .fieldDelimiter('|')
//
//                .includeFields("0110001000")
//                .tupleType(CPAResult.class);
        ;

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
