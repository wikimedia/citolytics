package de.tuberlin.dima.schubotz.wikisim.clickstream;

import de.tuberlin.dima.schubotz.wikisim.cpa.utils.WikiSimConfiguration;
import de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoEvaluation;
import de.tuberlin.dima.schubotz.wikisim.seealso.better.SeeAlsoInputMapper;
import de.tuberlin.dima.schubotz.wikisim.seealso.operators.BetterSeeAlsoLinkExistsFilter;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.com.google.common.collect.Sets;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Using Wikipedia ClickStream data set as relevance judgements.
 * <p/>
 * General information: http://meta.wikimedia.org/wiki/Research:Wikipedia_clickstream
 * Download: http://figshare.com/articles/Wikipedia_Clickstream/1305770
 * Examples: http://ewulczyn.github.io/Wikipedia_Clickstream_Getting_Started/
 * <p/>
 * Fields: rev_id, curr_id, n, prev_title, curr_title, type
 */
public class ClickStreamHelper {
    public final static HashSet<String> filterNameSpaces = Sets.newHashSet(
            "other-wikipedia", "other-empty", "other-internal", "other-google", "other-yahoo",
            "other-bing", "other-facebook", "other-twitter", "other-other"
    );

    public final static String filterType = "link";

    public static DataSet<Tuple3<String, String, Integer>> getClickStreamDataSet(ExecutionEnvironment env, String filename) {
        return env.readTextFile(filename)
                .flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        String[] cols = s.split(Pattern.quote("\t"));

                        // Replace underscores?
                        // e.g. 'Tis_So_Sweet_to_Trust_in_Jesus

                        if (cols.length == 6) {
                            if (filterType.equals(cols[5]) && !filterNameSpaces.contains(cols[3])) {
                                out.collect(new Tuple3<>(
                                        cols[3],
                                        cols[4],
                                        cols[2].isEmpty() ? 0 : Integer.valueOf(cols[2])
                                ));
                            }
                        } else {
                            throw new Exception("Wrong column length: " + cols.length + "; " + Arrays.toString(cols));
                        }
                    }
                })
                ;
    }

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length < 4) {
            System.err.print("Error: Parameter missing! Required: CLICKSTREAM SEEALSO LINKS OUTPUT, Current: " + Arrays.toString(args));
            System.exit(1);
        }

        String linksFilename = args[2];
        String outputFilename = args[3];

        // prev_title, curr_title, n
        DataSet<Tuple3<String, String, Integer>> clickStream = getClickStreamDataSet(env, args[0]);

        DataSet<Tuple2<String, ArrayList<String>>> seeAlso = env.readTextFile(args[1])
                .map(new SeeAlsoInputMapper());

        DataSet<Tuple2<String, HashSet<String>>> links = SeeAlsoEvaluation.getLinkDataSet(env, linksFilename);

        DataSet<Tuple3<String, String, Integer>> res = seeAlso
                .coGroup(links)
                .where(0)
                .equalTo(0)
                .with(new BetterSeeAlsoLinkExistsFilter())
                .coGroup(clickStream)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<Tuple2<String, ArrayList<String>>, Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, ArrayList<String>>> seeAlso, Iterable<Tuple3<String, String, Integer>> clickStream, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        Iterator<Tuple2<String, ArrayList<String>>> seeAlsoIterator = seeAlso.iterator();
                        Iterator<Tuple3<String, String, Integer>> clickStreamIterator = clickStream.iterator();

                        if (seeAlsoIterator.hasNext()) {
                            Tuple2<String, ArrayList<String>> seeAlsoRecord = seeAlsoIterator.next();
                            String article = seeAlsoRecord.getField(0);
                            ArrayList<String> linkList = seeAlsoRecord.getField(1);

                            HashMap<String, Integer> clickStreamMap = new HashMap<>();

                            while (clickStreamIterator.hasNext()) {
                                Tuple3<String, String, Integer> clickStreamRecord = clickStreamIterator.next();

                                clickStreamMap.put((String) clickStreamRecord.getField(1), (Integer) clickStreamRecord.getField(2));
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

        env.execute("ClickStream Evaluation");
    }
}
