package de.tuberlin.dima.schubotz.cpa.clickstream;

import de.tuberlin.dima.schubotz.cpa.evaluation.BetterEvaluation;
import de.tuberlin.dima.schubotz.cpa.evaluation.better.SeeAlsoInputMapper;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.BetterSeeAlsoLinkExistsFilter;
import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by malteschwarzer on 09.05.15.
 */
public class ClickStream {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length < 4) {
            System.err.print("Error: Parameter missing! Required: CLICKSTREAM SEEALSO LINKS OUTPUT, Current: " + Arrays.toString(args));
            System.exit(1);
        }

        String linksFilename = args[2];
        String outputFilename = args[3];

        // prev_id, curr_id, n, prev_title, curr_title
        DataSet<Tuple5<Integer, Integer, Integer, String, String>> clickStream = env.readTextFile(args[0])

                .map(new MapFunction<String, Tuple5<Integer, Integer, Integer, String, String>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer, String, String> map(String s) throws Exception {
                        String[] cols = s.split(Pattern.quote("\t"));

                        if (cols.length == 5) {
                            return new Tuple5<>(
                                    cols[0].isEmpty() ? 0 : Integer.valueOf(cols[0]),
                                    cols[1].isEmpty() ? 0 : Integer.valueOf(cols[1]),
                                    cols[2].isEmpty() ? 0 : Integer.valueOf(cols[2]),
                                    cols[3],
                                    cols[4]
                            );
                        } else {
                            throw new Exception("Wrong column length: " + cols.length + "; " + Arrays.toString(cols));
                        }
                    }
                });

        DataSet<Tuple2<String, ArrayList<String>>> seeAlso = env.readTextFile(args[1])
                .map(new SeeAlsoInputMapper());

        DataSet<Tuple2<String, HashSet<String>>> links = BetterEvaluation.getLinkDataSet(env, linksFilename);

        DataSet<Tuple3<String, String, Integer>> res = seeAlso
                .coGroup(links)
                .where(0)
                .equalTo(0)
                .with(new BetterSeeAlsoLinkExistsFilter())
                .coGroup(clickStream)
                .where(0)
                .equalTo(3)
                .with(new CoGroupFunction<Tuple2<String, ArrayList<String>>, Tuple5<Integer, Integer, Integer, String, String>, Tuple3<String, String, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, ArrayList<String>>> seeAlso, Iterable<Tuple5<Integer, Integer, Integer, String, String>> clickStream, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        Iterator<Tuple2<String, ArrayList<String>>> seeAlsoIterator = seeAlso.iterator();
                        Iterator<Tuple5<Integer, Integer, Integer, String, String>> clickStreamIterator = clickStream.iterator();

                        if (seeAlsoIterator.hasNext()) {
                            Tuple2<String, ArrayList<String>> seeAlsoRecord = seeAlsoIterator.next();
                            String article = seeAlsoRecord.getField(0);
                            ArrayList<String> linkList = seeAlsoRecord.getField(1);

                            HashMap<String, Integer> clickStreamMap = new HashMap<>();

                            while (clickStreamIterator.hasNext()) {
                                Tuple5<Integer, Integer, Integer, String, String> clickStreamRecord = clickStreamIterator.next();

                                clickStreamMap.put((String) clickStreamRecord.getField(4), (Integer) clickStreamRecord.getField(2));
                            }

                            for (String link : linkList) {
                                out.collect(new Tuple3<>(
                                        article,
                                        link,
                                        clickStreamMap.containsKey(link) ? clickStreamMap.get(link) : 0
                                ));
                            }

                        }
                    }
                });


        if (outputFilename.equals("print")) {
            res.print();
        } else {
            res.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, String.valueOf(WikiSimConfiguration.csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("WikiSimRedirects");
    }
}
