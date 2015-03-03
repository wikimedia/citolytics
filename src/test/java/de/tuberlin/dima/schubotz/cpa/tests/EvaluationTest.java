package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.evaluation.Evaluation;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.MatchesCounter;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResult;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

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
        StringListValue seealso = StringListValue.valueOf(new String[]{"A", "B", "C", "D"});
        StringListValue cpa = StringListValue.valueOf(new String[]{"E", "B"});

        System.out.println(
                MatchesCounter.getMeanReciprocalRank(cpa, seealso)
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
}
