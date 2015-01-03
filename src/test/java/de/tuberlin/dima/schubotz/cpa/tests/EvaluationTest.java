package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.evaluation.Evaluation;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.EvaluationOuterJoin;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.EvaluationFinalResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.EvaluationResult;
import de.tuberlin.dima.schubotz.cpa.types.StringListValue;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;

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
//                "file://" + getClass().getClassLoader().getResources("evaluation.out").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("testresult.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_mlt.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_links.csv").nextElement().getPath(),

                "n"
        });
    }

    @Test
    public void OutJoinTest() throws Exception {

        ArrayList<EvaluationFinalResult> first = new ArrayList<>();

        first.add(new EvaluationFinalResult("foo", new String[]{"bar", "x", "y"}));

        ArrayList<EvaluationResult> second = new ArrayList<>();

        second.add(new EvaluationResult("foo", new String[]{"q", "bar", "y", null, null}, 3));

        Collector<EvaluationFinalResult> collector = new Collector<EvaluationFinalResult>() {
            @Override
            public void collect(EvaluationFinalResult record) {

                System.out.println(record);
            }

            @Override
            public void close() {

            }
        };

        new EvaluationOuterJoin(new int[]{10, 5, 1}, EvaluationFinalResult.COCIT_LIST_KEY, EvaluationFinalResult.COCIT_MATCHES_KEY)
                .coGroup(first, second, collector);

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

    @Test
    public void ListTest() {


        StringListValue listA = StringListValue.valueOf(new String[]{"x", "w"});
        StringListValue listB = StringListValue.valueOf(new String[]{"v", "w", "x", "y", "z"});

        for (int i = 0; i < 50; i++) {
            System.out.println(ListUtils.intersection(listA, listB));
        }

    }
}
