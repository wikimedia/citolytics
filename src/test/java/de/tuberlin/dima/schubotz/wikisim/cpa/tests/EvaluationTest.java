package de.tuberlin.dima.schubotz.wikisim.cpa.tests;

import de.tuberlin.dima.schubotz.wikisim.clickstream.ClickStreamEvaluation;
import de.tuberlin.dima.schubotz.wikisim.clickstream.ClickStreamHelper;
import de.tuberlin.dima.schubotz.wikisim.clickstream.ClickStreamStats;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.LinkTuple;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.list.StringListValue;
import de.tuberlin.dima.schubotz.wikisim.histogram.EvaluationHistogram;
import de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoEvaluation;
import de.tuberlin.dima.schubotz.wikisim.seealso.better.ResultCoGrouper;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.wikisim.seealso.utils.EvaluationMeasures;
import junit.framework.Assert;
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
    public void TestClickStream() throws Exception {
        ClickStreamHelper.main(
                new String[]{
                        "file://" + getClass().getClassLoader().getResources("2015_02_clickstream_preview.tsv").nextElement().getPath(),
                        "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath(),
                        "file://" + getClass().getClassLoader().getResources("evaluation_links.csv").nextElement().getPath(),

                        "print"
                }
        );
    }

    @Test
    public void TestClickStreamEvaluation() throws Exception {
        ClickStreamEvaluation.main(
                new String[]{
                        "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath(),
                        "file://" + getClass().getClassLoader().getResources("2015_02_clickstream_preview.tsv").nextElement().getPath(),
                        "print",
                        "print",
                        "nofilter"
                }
        );
    }

    @Test
    public void TestClickStreamStats() throws Exception {
        ClickStreamStats.main(
                new String[]{
                        "file://" + getClass().getClassLoader().getResources("2015_02_clickstream_preview.tsv").nextElement().getPath(),
                        "print"
                }
        );
    }

    @Test
    public void HistogramTest() throws Exception {

        EvaluationHistogram.main(new String[]{
                "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath(),
                "print",
                "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath()

//                ,"y"
        });
    }

    @Test
    public void EvalCPATest() throws Exception {

        SeeAlsoEvaluation.main(new String[]{
                "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath(),
//                "file://" + getClass().getClassLoader().getResources("evaluation_mlt.csv").nextElement().getPath(),
                "print",
                "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath(),
                "nofilter"
//                "file://" + getClass().getClassLoader().getResources("evaluation_links.csv").nextElement().getPath()
//                ,"y"
        });
    }

    @Test
    public void EvalMLTTest() throws Exception {

        SeeAlsoEvaluation.main(new String[]{
//                "file://" + getClass().getClassLoader().getResources("testresult2.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_mlt.csv").nextElement().getPath(),
                "print",
                "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_links.csv").nextElement().getPath()
                , "2"
                , "0"
                , "1"
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

        WikiSimComparableResult<Double> testItemLow = new WikiSimComparableResult<>("B", 2.0);
        WikiSimComparableResult<Double> testItemHigh = new WikiSimComparableResult<>("A", 5.0);

        queue.add(testItemLow);
        queue.add(testItemLow);
        queue.add(testItemLow);
        queue.add(testItemLow);
        queue.add(testItemHigh);
        queue.add(new WikiSimComparableResult<>("A", 4.0));
        queue.add(new WikiSimComparableResult<>("A", 3.0));
        queue.add(new WikiSimComparableResult<>("A", 2.0));
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
        WikiSimComparableResult<Double> testItemLow = new WikiSimComparableResult<>("B", 2.0);
        WikiSimComparableResult<Double> testItemHigh = new WikiSimComparableResult<>("A", 5.0);

        List<WikiSimComparableResult<Double>> unsortedList = new ArrayList<>();

        unsortedList.add(testItemLow);
        unsortedList.add(new WikiSimComparableResult<>("A", 2.0));
        unsortedList.add(testItemHigh);
        unsortedList.add(new WikiSimComparableResult<>("A", 3.0));
        unsortedList.add(new WikiSimComparableResult<>("A", 4.0));


        List<WikiSimComparableResult<Double>> sortedList = Ordering.natural().greatestOf(unsortedList, 4);


        System.out.println("Unsorted: " + unsortedList);
        System.out.println("Sorted: " + sortedList);

        if (sortedList.get(0) != testItemHigh) {
            throw new Exception("testItemHigh is not first item: " + testItemHigh);
        }

        if (sortedList.contains(testItemLow)) {
            throw new Exception("testItemLow should not be in sortedList: " + testItemLow);
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

        retrieved.add(new WikiSimComparableResult<>("Hole opener", 3.0517578125E-5));
        retrieved.add(new WikiSimComparableResult<>("Drilling stabilizer", 9.313225746154785E-10));
        retrieved.add(new WikiSimComparableResult<>("Drill bit", 2.1268224907304786E-12));
        retrieved.add(new WikiSimComparableResult<>("Drag bit", 2.8421709430404007E-14));
        retrieved.add(new WikiSimComparableResult<>("Driller (oil)", 1.0E-15));
        retrieved.add(new WikiSimComparableResult<>("Drilling rig", 6.490547151887447E-17));

        retrieved.add(new WikiSimComparableResult<>("Tungsten carbide", 3.1475538306147624E-25));
        retrieved.add(new WikiSimComparableResult<>("Well bore", 5.986591089105288E-27));

        retrieved = Ordering.natural().greatestOf(retrieved, 10);

        // 8 retrieved
        System.out.println("Retrieved = " + retrieved.size() + " : " + retrieved);

        double hrr = EvaluationMeasures.getHarmonicReciprocalRank(ResultCoGrouper.getResultNamesAsList(retrieved), relevant);
        double map = EvaluationMeasures.getMeanAveragePrecision(ResultCoGrouper.getResultNamesAsList(retrieved), relevant);
        int[] matches = EvaluationMeasures.getMatchesCount(ResultCoGrouper.getResultNamesAsList(retrieved), relevant);

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

    @Deprecated
    public void doubleTest() {
        int a = 1;
        int b = 8;

        double d = ((double) a) / ((double) b);
        System.out.println(d);

    }

    @Deprecated
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
    public void HashTest() throws Exception {
        String pageA = "Foo";
        String pageB = "Bar";

        if (LinkTuple.getHash(pageA, pageB) != LinkTuple.getHash(pageA, pageB))
            throw new Exception("Hash not the same.");
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
