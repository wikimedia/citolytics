package de.tuberlin.dima.schubotz.cpa.histogram;

import de.tuberlin.dima.schubotz.cpa.WikiSim;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.cpa.types.list.StringListValue;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.regex.Pattern;

public class EvaluationHistogram {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println(new WikiSim().getDescription());
            System.exit(1);
        }

        String inputFilename = args[0];
        String outputFilename = args[1];
        String seeAlsoFilename = args[2];

        Configuration config = new Configuration();

        config.setBoolean("parseInput", false); //(args.length > 2 && args[2].equals("y") ? true : false));

        DataSet<Tuple2<String, StringListValue>> seeAlsoDataSet = env.readTextFile(seeAlsoFilename)
                .map(new MapFunction<String, Tuple2<String, StringListValue>>() {
                    @Override
                    public Tuple2<String, StringListValue> map(String s) throws Exception {
                        String[] cols = s.split(Pattern.quote("|"));
                        String[] links = cols[1].split(Pattern.quote("#"));

                        return new Tuple2<>(cols[0], StringListValue.valueOf(links));
                    }
                });

//        DataSet<Long> wikisimDataSetX = env.readTextFile(inputFilename)
//                .map(new RichMapFunction<String, Long>() {
//                    private boolean parseInput = false;
//                    @Override
//                    public void open(Configuration parameter) throws Exception {
//                        super.open(parameter);
//
//                        parseInput = parameter.getBoolean("parseInput", false);
//                    }
//
//                    @Override
//                    public Long map(String s) throws Exception {
//                        if (parseInput) {
//                            String[] cols = s.split(Pattern.quote("|"));
//                        }
//
//                        return new Long(1);
//                    }
//                })
//                .withParameters(config)
//                .reduce(new ReduceFunction<Long>() {
//                    @Override
//                    public Long reduce(Long a, Long b) throws Exception {
//                        return a + b;
//                    }
//                });

        DataSet<Tuple3<String, Integer, Integer>> wikisimDataSet = env.readTextFile(inputFilename)
                .flatMap(new FlatMapFunction<String, Tuple3<String, String, Double>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple3<String, String, Double>> out) throws Exception {
                        String[] cols = s.split(Pattern.quote("|"));

                        out.collect(new Tuple3<>(cols[1], cols[2], Double.valueOf(cols[8])));
                        out.collect(new Tuple3<>(cols[2], cols[1], Double.valueOf(cols[8])));
                    }
                })

                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple3<String, String, Double>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<String, String, Double>> in, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        Iterator<Tuple3<String, String, Double>> iterator = in.iterator();

                        Tuple3<String, String, Double> joinRecord = null;

                        int count = 0;
                        MinMaxPriorityQueue<WikiSimComparableResult<Double>> unsortedQueue = MinMaxPriorityQueue.maximumSize(10).create();

                        while (iterator.hasNext()) {
                            joinRecord = iterator.next();
                            unsortedQueue.add(new WikiSimComparableResult<Double>((String) joinRecord.getField(0), (Double) joinRecord.getField(2)));
                            count++;
                        }

//                        System.out.println(unsortedQueue);

                        out.collect(new Tuple3<>((String) joinRecord.getField(0), 1, count));
                    }
                });
//                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
//                    @Override
//                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> a, Tuple3<String, Integer, Integer> b) throws Exception {
//                        int max;
//
//                        if(((int) a.getField(2)) > ((int) b.getField(2)))
//                            max = a.getField(2);
//                        else
//                            max =  b.getField(2);
//
//                        return new Tuple3<>("0:total; 1:max", ((int) a.getField(1)) + ((int) b.getField(1)), max);
//                    }
//                })
//                .aggregate(Aggregations.MAX, 2);

        DataSet<Tuple5<String, StringListValue, Integer, StringListValue, Integer>> output = seeAlsoDataSet
                .coGroup(wikisimDataSet)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<String, StringListValue>, Tuple3<String, Integer, Integer>, Tuple5<String, StringListValue, Integer, StringListValue, Integer>>() {

                    @Override
                    public void coGroup(Iterable<Tuple2<String, StringListValue>> a, Iterable<Tuple3<String, Integer, Integer>> b, Collector<Tuple5<String, StringListValue, Integer, StringListValue, Integer>> out) throws Exception {
                        Iterator<Tuple2<String, StringListValue>> iteratorA = a.iterator();
                        Iterator<Tuple3<String, Integer, Integer>> iteratorB = b.iterator();

                        if (iteratorA.hasNext()) {
                            Tuple2<String, StringListValue> recordA = iteratorA.next();
                            StringListValue joinList = new StringListValue();

                            while (iteratorB.hasNext()) {
                                Tuple3<String, Integer, Integer> recordB = iteratorB.next();

                                joinList.add(new StringValue((String) recordB.getField(0)));
                            }

                            out.collect(new Tuple5<>(
                                    (String) recordA.getField(0),
                                    (StringListValue) recordA.getField(1),
                                    ((StringListValue) recordA.getField(1)).size(),
                                    joinList,
                                    joinList.size()
                            ));
                        }
                    }
                });

        if (outputFilename.equals("print")) {
            output.print();
        } else {
            output.writeAsText(outputFilename, FileSystem.WriteMode.OVERWRITE);
        }


        env.execute("EvaluationPerformanceTest: Group by + MinMax queue + SeeAlso join");
//                    env.execute("EvaluationHistogram - Count lines");
    }
}
