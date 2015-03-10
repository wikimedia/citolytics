package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.evaluation.BetterEvaluation;
import de.tuberlin.dima.schubotz.cpa.evaluation.Evaluation;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.utils.EvaluationMeasures;
import de.tuberlin.dima.schubotz.cpa.histogram.EvaluationHistogram;
import de.tuberlin.dima.schubotz.cpa.types.list.StringListValue;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.flink.shaded.com.google.common.collect.Ordering;
import org.junit.Test;

import java.util.*;

public class EvaluationTest {

    @Test
    public void LocalTest() throws Exception {

        Evaluation.main(new String[]{
                "print",
//                "file://" + getClass().getClassLoader().getResources("evaluation.out").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_mlt.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_links.csv").nextElement().getPath()
                // first N
                , "1"
                // cpa key
                , "8"
                // aggregate
//                "n"
        });
    }

    @Test
    public void HistogramTest() throws Exception {

        EvaluationHistogram.main(new String[]{
                "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath(),
                "print"
//                ,"y"
        });
    }

    @Test
    public void EvalPerformanceTest() throws Exception {

        BetterEvaluation.main(new String[]{
                "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath(),
                "print",
                "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath()

//                ,"y"
        });
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

    @Deprecated
    public void ListTest() {


        StringListValue listA = StringListValue.valueOf(new String[]{"x", "w"});
        StringListValue listB = StringListValue.valueOf(new String[]{"v", "w", "x", "y", "z"});

        for (int i = 0; i < 50; i++) {
            System.out.println(ListUtils.intersection(listA, listB));
        }

    }

    @Deprecated
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

    @Deprecated
    public void MRRTest() {
        List<String> seealso = Arrays.asList(new String[]{"A", "B", "C", "D"});
        List<String> cpa = Arrays.asList(new String[]{"E", "B"});

        System.out.println(
                EvaluationMeasures.getMeanReciprocalRank(cpa, seealso)
        );


        double d = 1 / 2;
        System.out.println(
                d
        );
    }

    @Test
    public void MinMaxQueueTest() {
        int maxListLength = 4;
        MinMaxPriorityQueue<WikiSimComparableResult<Double>> queue = MinMaxPriorityQueue

                .maximumSize(maxListLength).create();

        queue.add(new WikiSimComparableResult<>("A", 5.0));
        queue.add(new WikiSimComparableResult<>("A", 4.0));
        queue.add(new WikiSimComparableResult<>("A", 3.0));
        queue.add(new WikiSimComparableResult<>("A", 2.0));
        queue.add(new WikiSimComparableResult<>("B", 2.0));

        System.out.println(queue);


        MinMaxPriorityQueue<Integer> q = MinMaxPriorityQueue
                .orderedBy(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o1.compareTo(o2);
                    }
                })
                .maximumSize(4).create();

        q.add(1);
        q.add(2);
        q.add(3);
        q.add(4);
        q.add(5);
        q.add(6);
        q.add(1);
        q.add(1);


        System.out.println(q);
        System.out.println(Ordering.natural().greatestOf(q, 4));
    }

    @Test
    public void unionTest() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();

        list.add(new Tuple2<String, Integer>("A", 1));
        list.add(new Tuple2<String, Integer>("A", 2));

        DataSet<Tuple2<String, Collection<Tuple2<String, Integer>>>> a = env.fromElements(
                new Tuple2<String, Collection<Tuple2<String, Integer>>>("a", list),
                new Tuple2<String, Collection<Tuple2<String, Integer>>>("b", list)
        );

        DataSet<Tuple3<String, String, Collection<Tuple2<String, Integer>>>> b = env.fromElements(
                new Tuple3<String, String, Collection<Tuple2<String, Integer>>>("x", "fff", list),
                new Tuple3<String, String, Collection<Tuple2<String, Integer>>>("y", "fff", list)
        );


        DataSet<Tuple2<String, Collection<Tuple2<String, Integer>>>> c = b.project(1, 2);

        a.union(
                c
        ).print();

        env.execute("CSV Input test");
    }

    @Test
    public void collectionTest() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();

        list.add(new Tuple2<String, Integer>("A", 1));
        list.add(new Tuple2<String, Integer>("A", 2));

        DataSet<Tuple2<String, ArrayList<Tuple2<String, Integer>>>> res = env.fromElements(
                new Tuple2<String, ArrayList<Tuple2<String, Integer>>>("a", list),
                new Tuple2<String, ArrayList<Tuple2<String, Integer>>>("b", list)
        ).map(new MapFunction<Tuple2<String, ArrayList<Tuple2<String, Integer>>>, Tuple2<String, ArrayList<Tuple2<String, Integer>>>>() {
                  @Override
                  public Tuple2<String, ArrayList<Tuple2<String, Integer>>> map(Tuple2<String, ArrayList<Tuple2<String, Integer>>> in) throws Exception {

                      System.out.println("##" + in.getField(1));
                      return in;
                  }
              }

        );


        res.print();

        env.execute("CSV Input test");
    }

    public void pojoTest() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        class ListPojo {
            public String title;
            public List<String> results = new ArrayList<>();

            public ListPojo(String title, String... results) {
                this.title = title;

                for (int i = 0; i < results.length; i++) {
                    this.results.add(results[i]);
                }
            }
        }

        DataSet<ListPojo> res = env.fromElements(
                new ListPojo("A", new String[]{"a", "aa"}),
                new ListPojo("B", new String[]{"bbb", "bb"})

        );


        res.print();

        env.execute("CSV Input test");
    }

    @Test
    public void doubleTest() {
        int a = 1;
        int b = 8;

        double d = ((double) a) / ((double) b);
        System.out.println(d);

    }

    @Test
    public void parseDoublePerformanceTest2() {

        int runs = 999999;
        long start = System.nanoTime();
        for (int i = 0; i < runs; i++) {
            Double.valueOf("3.3489451534507196E-104");
        }
        long time = System.nanoTime() - start;
        System.out.printf("DD to double took an average of %.1f us%n", time / runs / 1000.0);

        long startB = System.nanoTime();
        for (int i = 0; i < runs; i++) {
            Double.valueOf("3.348");
        }
        long timeB = System.nanoTime() - startB;
        System.out.printf("II to double took an average of %.1f us%n", timeB / runs / 1000.0);

    }

    @Test
    public void parseDoublePerformanceTest() {

        int runs = 100000;

        String[] ints = new String[runs];
        String[] doubles = new String[runs];

        // 3.3489451534507196E-104

        for (int i = 0; i < runs; i++) {

            double doub = Math.random();
            int inte = (int) (doub * 100);

            doubles[i] = String.valueOf(doub);
            ints[i] = String.valueOf(inte);
        }

        long start = System.nanoTime();
        for (int x = 0; x < 100; x++) {
            for (String s : ints) {
                Double d = Double.valueOf(s);
            }
        }

        long time = System.nanoTime() - start;
        System.out.printf("Integer to double took an average of %.1f us%n", time / runs / 1000.0);


        long startB = System.nanoTime();
        for (int x = 0; x < 100; x++) {
            for (String s : doubles) {
                Double dd = Double.valueOf(s);
            }
        }

        long timeB = System.nanoTime() - startB;
        System.out.printf("Double to double took an average of %.1f us%n", timeB / runs / 1000.0);

    }

    @Test
    public void MinMaxQueuePerformanceTest() {

        int listlength = 100000;
        int runs = 1000;

        double[] ints = new double[listlength];
        double[] doubles = new double[listlength];


        for (int i = 0; i < listlength; i++) {

            double doub = Math.random();
            int inte = (int) (doub * 100);

            doubles[i] = doub;
            ints[i] = (double) inte;
        }

        MinMaxPriorityQueue<Double> queue = MinMaxPriorityQueue.maximumSize(10).create();
        long start = System.nanoTime();
        for (int x = 0; x < runs; x++) {
            for (double s : ints) {
                queue.add(Double.valueOf(s));
            }
        }

        long time = System.nanoTime() - start;
        System.out.printf("Adding Integer to MinMaxQueue took an average of %.1f us%n", time / (runs * listlength) / 1000.0);

        queue = MinMaxPriorityQueue.maximumSize(10).create();

        long startB = System.nanoTime();
        for (int x = 0; x < runs; x++) {
            for (double s : doubles) {
                queue.add(Double.valueOf(s));
            }
        }

        long timeB = System.nanoTime() - startB;
        System.out.printf("Adding Double to MinMaxQueue took an average of %.1f us%n", timeB / (runs * listlength) / 1000.0);

    }
}
