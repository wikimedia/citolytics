package de.tuberlin.dima.schubotz.cpa;

import de.tuberlin.dima.schubotz.cpa.types.StringListValue;
import de.tuberlin.dima.schubotz.cpa.types.WikiSimResultList;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.ListValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Evaluation
 * -> Count matches from result links and see also links
 * <p/>
 * Article | SeeAlso Links   | CoCit Links | CoCit Matches | CPA Links | CPA Matches |  MLT Links | MLT Matches
 * ---
 * Page1   | Page2, 3, 6, 7  |  3, 9       | 1             | 12, 3, 7  |    2        |  2, 3      | 2
 * ---
 * Sum ...
 * <p/>
 * ***********
 * <p/>
 * - Article
 * - SeeAlso Links (List String)
 * -- matches (int)
 * - CPA Links
 * -- matches
 * - CoCit Links
 * -- matches
 * - MLT Links
 * -- matches
 */
public class Evaluation {
    public static String csvRowDelimiter = "\n";
    public static char csvFieldDelimiter = '\t';

    public static int firstN = 3;

    public static void main(String[] args) throws Exception {

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println(new WikiSim().getDescription());
            System.exit(1);
        }

        String inputFilename = args[0];
        String outputFilename = args[1];

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Demo Data
        DataSet<Tuple3<String, String, Integer>> a = env.fromElements(
                new Tuple3<String, String, Integer>("A", "B", 2),
                new Tuple3<String, String, Integer>("A", "C", 5),
                new Tuple3<String, String, Integer>("A", "D", 7),
                new Tuple3<String, String, Integer>("A", "E", 9),
                new Tuple3<String, String, Integer>("A", "F", 1),
                new Tuple3<String, String, Integer>("B", "F", 2),
                new Tuple3<String, String, Integer>("B", "G", 1),
                new Tuple3<String, String, Integer>("C", "F", 2)


        );

        // Build link list
        DataSet<Tuple2<String, StringListValue>> r1 = a.groupBy(0)
                //.filter by (Link not exists + See Also count == x)
                .sortGroup(2, Order.DESCENDING)
                .first(firstN)
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple3<String, String, Integer>, Tuple2<String, StringListValue>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<String, String, Integer>> results, Collector<Tuple2<String, StringListValue>> out) throws Exception {
                        Iterator<Tuple3<String, String, Integer>> iterator = results.iterator();
                        Tuple3<String, String, Integer> record = null;
                        StringListValue list = new StringListValue();

                        while (iterator.hasNext()) {
                            record = iterator.next();
                            list.add(new StringValue((String) record.getField(1)));
                        }
                        out.collect(new Tuple2(record.getField(0), list));
                    }
                });


        // Result:  DataSet<Tuple2<String, List<String>>>
        DataSet<Tuple2<String, String>> b = env.fromElements(
                new Tuple2<String, String>("1", "A"),
                new Tuple2<String, String>("1", "B"),
                new Tuple2<String, String>("4", "D")
        );

        r1.print();

        env.execute("Evaluation");
    }
}
