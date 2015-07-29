package de.tuberlin.dima.schubotz.wikisim.seealso.better;

import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResultList;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.regex.Pattern;

/**
 * Reads the MoreLikeThis result set created by ResultCollector of https://github.com/mschwarzer/Wikipedia2Lucene
 */
public class MLTInputMapper extends RichFlatMapFunction<String, Tuple2<String, WikiSimComparableResultList<Double>>> {
    private static Logger LOG = Logger.getLogger(MLTInputMapper.class);

    private int topK = 20;
    private final Pattern delimiterPattern = Pattern.compile(Pattern.quote("|"));

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);

        topK = parameter.getInteger("topK", 20);
    }

    @Override
    public void flatMap(String s, Collector<Tuple2<String, WikiSimComparableResultList<Double>>> out) throws Exception {
        String[] cols = delimiterPattern.split(s);
        WikiSimComparableResultList<Double> results = new WikiSimComparableResultList<>();

        try {
            if ((cols.length - 1 % 2) == 0 || cols.length < 3) {
                throw new Exception("Invalid number of columns (" + cols.length + ")");
            }

            for (int c = 1; c < cols.length; c += 2) {
                results.add(new WikiSimComparableResult<>(cols[c], Double.valueOf(cols[c + 1])));

                if (results.size() >= topK) {
                    break;
                }
            }

            out.collect(new Tuple2<>(cols[0], results));
        } catch (Exception e) {
            LOG.error("Cannot parse line - " + e.getMessage() + "\n" + s);
        }

    }
}