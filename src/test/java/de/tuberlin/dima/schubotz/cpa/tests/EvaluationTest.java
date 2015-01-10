package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.evaluation.Evaluation;
import de.tuberlin.dima.schubotz.cpa.types.list.StringListValue;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
//                "file://" + getClass().getClassLoader().getResources("evaluation.out").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("testresult.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_mlt.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_links.csv").nextElement().getPath(),

                "n"
        });
    }

//    @Test
//    public void OutJoinTest() throws Exception {
//
//        ArrayList<GenericEvaluationFinalResult> first = new ArrayList<>();
//
//        first.add(new GenericEvaluationFinalResult("foo", new String[]{"bar", "x", "y"}));
//
//        ArrayList<EvaluationResult> second = new ArrayList<>();
//
//        second.add(new EvaluationResult("foo", new String[]{"q", "bar", "y", null, null}, 3));
//
//        Collector<GenericEvaluationFinalResult> collector = new Collector<GenericEvaluationFinalResult>() {
//            @Override
//            public void collect(GenericEvaluationFinalResult record) {
//
//                System.out.println(record);
//            }
//
//            @Override
//            public void close() {
//
//            }
//        };
//
//        new EvaluationOuterJoin(new int[]{10, 5, 1}, GenericEvaluationFinalResult.COCIT_LIST_KEY, GenericEvaluationFinalResult.COCIT_MATCHES_KEY)
//                .coGroup(first, second, collector);
//
//    }


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

    @Test
    public void SortFirstTest() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String, String, Integer>> input = env.fromElements(
                new Tuple3<String, String, Integer>("A", "a", 1),
                new Tuple3<String, String, Integer>("A", "aa", 1),

                new Tuple3<String, String, Integer>("B", "b", 2),
                new Tuple3<String, String, Integer>("B", "bb", 2),
                new Tuple3<String, String, Integer>("D", "d", 2));

        input.groupBy(0).sortGroup(2, Order.ASCENDING).first(1).print();

        env.execute("sort");
    }
}
