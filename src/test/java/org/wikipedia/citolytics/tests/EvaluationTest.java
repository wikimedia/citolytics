package org.wikipedia.citolytics.tests;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import junit.framework.Assert;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.clickstream.ClickStreamEvaluation;
import org.wikipedia.citolytics.clickstream.ClickStreamStats;
import org.wikipedia.citolytics.cpa.types.LinkTuple;
import org.wikipedia.citolytics.seealso.SeeAlsoEvaluation;
import org.wikipedia.citolytics.seealso.better.EvaluateSeeAlso;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.utils.EvaluationMeasures;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.util.*;

public class EvaluationTest extends Tester {

    @Ignore
    @Test
    public void TestClickStreamEvaluationCPA() throws Exception {
        ClickStreamEvaluation.main(("--input " + resource("wikisim_output.csv") + " --clickstream "
                + resource("2015_02_clickstream_preview.tsv") + " --output print --score 4").split(" ")
        );
    }

    @Ignore
    @Test
    public void TestClickStreamEvaluationMLT() throws Exception {
        ClickStreamEvaluation.main(
                ("--input " + resource("evaluation_mlt.csv") + " --clickstream "
                        + resource("2015_02_clickstream_preview.tsv") + " --output print").split(" ")

        );
    }

    @Ignore
    @Test
    public void TestClickStreamStats() throws Exception {
        ClickStreamStats.main(
                new String[]{
                        resource("2015_02_clickstream_preview.tsv"),
                        "print"
                }
        );
    }

    @Ignore
    @Test
    public void EvalCPATest() throws Exception {

        SeeAlsoEvaluation.main(new String[]{
                resource("wikisim_output.csv"),
//                "file://" + getClass().getClassLoader().getResources("evaluation_mlt.csv").nextElement().getPath(),
                "print",
                resource("evaluation_seealso.csv"),
                "nofilter"
                , "4"
                , "1"
                , "2"
//                "file://" + getClass().getClassLoader().getResources("evaluation_links.csv").nextElement().getPath()
//                ,"y"
        });
    }

    @Ignore
    @Test
    public void EvalMLTTest() throws Exception {

        SeeAlsoEvaluation.main(new String[]{
//                "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath(),
                resource("evaluation_mlt.csv"),
                "print",
                resource("evaluation_seealso.csv"),
                resource("evaluation_links.csv")
                , "-1"
        });
    }


    @Ignore
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

    @Ignore
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
    public void MinMaxQueueTest() throws Exception {
        int maxListLength = 4;
        MinMaxPriorityQueue<WikiSimComparableResult<Double>> queue = MinMaxPriorityQueue
                .orderedBy(new Comparator<WikiSimComparableResult<Double>>() {
                    @Override
                    public int compare(WikiSimComparableResult<Double> o1, WikiSimComparableResult<Double> o2) {
                        return -1 * o1.compareTo(o2);
                    }
                })
                .maximumSize(maxListLength).create();

        WikiSimComparableResult<Double> testItemLow = new WikiSimComparableResult<>("B", 2.0, 0);
        WikiSimComparableResult<Double> testItemHigh = new WikiSimComparableResult<>("A", 5.0, 0);

        queue.add(testItemLow);
        queue.add(testItemLow);
        queue.add(testItemLow);
        queue.add(testItemLow);
        queue.add(testItemHigh);
        queue.add(new WikiSimComparableResult<>("A", 4.0, 0));
        queue.add(new WikiSimComparableResult<>("A", 3.0,0));
        queue.add(new WikiSimComparableResult<>("A", 2.0,0));
        queue.add(testItemLow);

        System.out.println("Simple Queue" + queue);

        if (queue.contains(testItemLow)) {
            throw new Exception("testItemLow NOT should exist in queue: " + testItemLow);
        }

        if (!queue.contains(testItemHigh)) {
            throw new Exception("testItemHigh should exist in queue: " + testItemHigh);
        }

        if (testItemHigh.compareTo(testItemLow) != 1) {
            throw new Exception(testItemHigh + " compareTo " + testItemLow + " = " + testItemHigh.compareTo(testItemLow) + " // should be = 1");
        }

    }

    @Test
    public void GreatestOfTest() throws Exception {
        WikiSimComparableResult<Double> testItemLow = new WikiSimComparableResult<>("B", 2.0, 0);
        WikiSimComparableResult<Double> testItemHigh = new WikiSimComparableResult<>("A", 5.0, 0);

        List<WikiSimComparableResult<Double>> unsortedList = new ArrayList<>();

        unsortedList.add(testItemLow);
        unsortedList.add(new WikiSimComparableResult<>("A", 2.0, 0));
        unsortedList.add(testItemHigh);
        unsortedList.add(new WikiSimComparableResult<>("A", 4.0,0));
        unsortedList.add(new WikiSimComparableResult<>("A", 3.0, 0));


        List<WikiSimComparableResult<Double>> sortedList = Ordering.natural().greatestOf(unsortedList, 4);


        System.out.println("Unsorted: " + unsortedList);
        System.out.println("Sorted: " + sortedList);

        if (sortedList.get(0) != testItemHigh) {
            throw new Exception("testItemHigh is not first item: " + testItemHigh);
        }

        if (sortedList.contains(testItemLow)) {
            throw new Exception("testItemLow should not be in sortedList: " + testItemLow);
        }

        // check order
        WikiSimComparableResult<Double> prev = null;
        for (WikiSimComparableResult<Double> current : sortedList) {
            if (prev == null) {
                prev = current;
            } else if (current.compareTo(prev) > 0) {
                throw new Exception("SortedList in wrong order. Prev = " + prev.toString() + "; Current = " + current.toString());
            }
        }

    }

    @Test
    public void MAPTest() {

        List<String> retrieved = Arrays.asList("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20".split(","));
        HashSet<String> relevant = new HashSet<>();

        relevant.addAll(Arrays.asList("1,3,6,10,20".split(",")));

        double map = EvaluationMeasures.getMeanAveragePrecision(retrieved, relevant);

        Assert.assertEquals("MAP is wrong (MAP * 10000)", 5633, Math.round(map * 10000));
    }

    @Test
    public void MatchTest() {

        List<String> relevant = Arrays.asList("Drag bit,Driller (oil),Drill bit,Drilling stabilizer,Drilling rig,Hole opener".split(","));

        List<WikiSimComparableResult<Double>> retrieved = new ArrayList<>();

        retrieved.add(new WikiSimComparableResult<>("Hole opener", 3.0517578125E-5, 0));
        retrieved.add(new WikiSimComparableResult<>("Drilling stabilizer", 9.313225746154785E-10, 0));
        retrieved.add(new WikiSimComparableResult<>("Drill bit", 2.1268224907304786E-12, 0));
        retrieved.add(new WikiSimComparableResult<>("Drag bit", 2.8421709430404007E-14, 0));
        retrieved.add(new WikiSimComparableResult<>("Driller (oil)", 1.0E-15, 0));
        retrieved.add(new WikiSimComparableResult<>("Drilling rig", 6.490547151887447E-17, 0));

        retrieved.add(new WikiSimComparableResult<>("Tungsten carbide", 3.1475538306147624E-25, 0));
        retrieved.add(new WikiSimComparableResult<>("Well bore", 5.986591089105288E-27, 0));

        retrieved = Ordering.natural().greatestOf(retrieved, 10);

        // 8 retrieved
        System.out.println("Retrieved = " + retrieved.size() + " : " + retrieved);

        double hrr = EvaluationMeasures.getHarmonicReciprocalRank(EvaluateSeeAlso.getResultNamesAsList(retrieved), relevant);
        double map = EvaluationMeasures.getMeanAveragePrecision(EvaluateSeeAlso.getResultNamesAsList(retrieved), relevant);
        int[] matches = EvaluationMeasures.getMatchesCount(EvaluateSeeAlso.getResultNamesAsList(retrieved), relevant);

        System.out.println("HRR = " + hrr);
        System.out.println("MAP = " + map);

        System.out.println("MatchesCount = " + Arrays.toString(matches));

        Assert.assertEquals("MatchesCount is wrong", 6, matches[0]);
        Assert.assertEquals("MatchesCount is wrong", 5, matches[1]);
        Assert.assertEquals("MatchesCount is wrong", 1, matches[2]);

        Assert.assertEquals("HRR is wrong", 1.0000000000000002, hrr);
        Assert.assertEquals("MAP is wrong", 1.0, map);
    }

    @Deprecated
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

    @Deprecated
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
    public void HashTest() throws Exception {
        String pageA = "Foo";
        String pageB = "Bar";

        if (LinkTuple.getHash(pageA, pageB) != LinkTuple.getHash(pageA, pageB))
            throw new Exception("Hash not the same.");
    }

}
