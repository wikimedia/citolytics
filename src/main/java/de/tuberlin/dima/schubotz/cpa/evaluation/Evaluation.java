package de.tuberlin.dima.schubotz.cpa.evaluation;

import de.tuberlin.dima.schubotz.cpa.WikiSim;
import de.tuberlin.dima.schubotz.cpa.types.StringListValue;
import de.tuberlin.dima.schubotz.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.cpa.types.WikiSimResultList;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
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
    public static char csvFieldDelimiter = '|';

    public static int firstN = 16;

    public static void main(String[] args) throws Exception {

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println(new WikiSim().getDescription());
            System.exit(1);
        }


        String seeAlsoInput = args[0];
        String wikiSimInput = args[1];
        String outputFilename = args[2];

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Prepare CPA
        DataSet<CPAResult> wikiSimResults = env.readCsvFile(wikiSimInput)
                .fieldDelimiter(csvFieldDelimiter)

                .includeFields("0110001000")
                .tupleType(CPAResult.class);

        DataSet<Tuple2<String, StringListValue>> firstCpaResults = wikiSimResults.groupBy(0)
                //.filter by (Link not exists + See Also count == x)
                .sortGroup(2, Order.DESCENDING)
                .first(firstN)
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<CPAResult, Tuple2<String, StringListValue>>() {
                    @Override
                    public void reduce(Iterable<CPAResult> results, Collector<Tuple2<String, StringListValue>> out) throws Exception {
                        Iterator<CPAResult> iterator = results.iterator();
                        CPAResult record = null;
                        StringListValue list = new StringListValue();

                        while (iterator.hasNext()) {
                            record = iterator.next();
                            list.add(new StringValue((String) record.getField(1)));
                        }
                        out.collect(new Tuple2(record.getField(0), list));
                    }
                });

        // Prepare SeeAlso
        DataSet<Tuple2<String, StringListValue>> seeAlsoResults = env.readCsvFile(seeAlsoInput)
                .fieldDelimiter(csvFieldDelimiter)
                .tupleType(SeeAlsoResult.class)
                .map(new MapFunction<SeeAlsoResult, Tuple2<String, StringListValue>>() {
                    @Override
                    public Tuple2<String, StringListValue> map(SeeAlsoResult in) throws Exception {
                        String[] list = ((String) in.getField(1)).split(",");
                        return new Tuple2<String, StringListValue>((String) in.getField(0), StringListValue.valueOf(list));
                    }
                });

        // Join SeeAlso x CPA
        DataSet<Tuple4<String, Integer, StringListValue, StringListValue>> join = seeAlsoResults
                .join(firstCpaResults)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<String, StringListValue>, Tuple2<String, StringListValue>, Tuple4<String, Integer, StringListValue, StringListValue>>() {
                    @Override
                    public Tuple4<String, Integer, StringListValue, StringListValue> join(Tuple2<String, StringListValue> first, Tuple2<String, StringListValue> second) throws Exception {

                        //
                        int matches = ListUtils.intersection((StringListValue) first.getField(1), (StringListValue) second.getField(1)).size();

                        return new Tuple4<String, Integer, StringListValue, StringListValue>((String) first.getField(0), matches, (StringListValue) first.getField(1), (StringListValue) second.getField(1));
                    }
                });

        // Build link list
//        DataSet<Tuple2<String, StringListValue>> r1 = a.groupBy(0)
//                //.filter by (Link not exists + See Also count == x)
//                .sortGroup(2, Order.DESCENDING)
//                .first(firstN)
//                .groupBy(0)
//                .reduceGroup(new GroupReduceFunction<Tuple3<String, String, Integer>, Tuple2<String, StringListValue>>() {
//                    @Override
//                    public void reduce(Iterable<Tuple3<String, String, Integer>> results, Collector<Tuple2<String, StringListValue>> out) throws Exception {
//                        Iterator<Tuple3<String, String, Integer>> iterator = results.iterator();
//                        Tuple3<String, String, Integer> record = null;
//                        StringListValue list = new StringListValue();
//
//                        while (iterator.hasNext()) {
//                            record = iterator.next();
//                            list.add(new StringValue((String) record.getField(1)));
//                        }
//                        out.collect(new Tuple2(record.getField(0), list));
//                    }
//                });
//        seeAlsoResults.print();

        join.writeAsCsv(outputFilename, csvRowDelimiter, String.valueOf(csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        join.print();

        env.execute("Evaluation");
    }
}
