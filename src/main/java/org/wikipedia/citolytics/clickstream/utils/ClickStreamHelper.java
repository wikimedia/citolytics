package org.wikipedia.citolytics.clickstream.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.clickstream.operators.ClickStreamDataSetReader;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTranslateTuple;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;
import org.wikipedia.citolytics.multilang.LangLinkTuple;
import org.wikipedia.citolytics.multilang.MultiLang;

import java.util.HashMap;

/**
 * Using Wikipedia ClickStream data set as relevance judgements.
 *
 * @link General information: http://meta.wikimedia.org/wiki/Research:Wikipedia_clickstream
 * @link Download: http://figshare.com/articles/Wikipedia_Clickstream/1305770
 * @link Examples: http://ewulczyn.github.io/Wikipedia_Clickstream_Getting_Started/
 *
 * Fields: rev_id, curr_id, n, prev_title (referrer), curr_title, type
 */
public class ClickStreamHelper {

    /**
     * Returns data set of click stream tuples
     *
     * @param env ExecutionEnvironment
     * @param filename Path to data set (separate multiple files by comma)
     * @return
     */
    public static DataSet<ClickStreamTuple> getClickStreamDataSet(ExecutionEnvironment env, String filename) {
        return getTranslatedClickStreamDataSet(env, filename, null, null);
    }

    public static DataSet<ClickStreamTuple> getTranslatedClickStreamDataSet(ExecutionEnvironment env, String filename, String lang, String langLinksFilename) {
        DataSet<ClickStreamTranslateTuple> translateInput = readClickStreamDataSetInputs(env, filename);

        // Translate if requested
        if(lang != null && langLinksFilename != null) {

            // Load enwiki language links
            DataSet<LangLinkTuple> langLinks = MultiLang.readLangLinksDataSet(env, langLinksFilename, lang);

            // Translate article name and target name
            translateInput = translateInput
                // article name
                .join(langLinks)
                .where(ClickStreamTranslateTuple.ARTICLE_ID_KEY)
                .equalTo(LangLinkTuple.PAGE_ID_KEY)
                .with((JoinFunction<ClickStreamTranslateTuple, LangLinkTuple, ClickStreamTranslateTuple>) (cs, ll) -> {
                    // Replace names with translated values
                    System.out.println("TRANLATE (articleName) " + cs.getArticleName() + " => " + ll.getTargetTitle());
                    cs.setField(ll.getTargetTitle(), ClickStreamTranslateTuple.ARTICLE_NAME_KEY);
                    return cs;
                })
                // target name
                .join(langLinks)
                .where(ClickStreamTranslateTuple.TARGET_ID_KEY)
                .equalTo(LangLinkTuple.PAGE_ID_KEY)
                .with((JoinFunction<ClickStreamTranslateTuple, LangLinkTuple, ClickStreamTranslateTuple>) (cs, ll) -> {
                    // Replace names with translated values
                    System.out.println("TRANLATE (targetName) " + cs.getTargetName() + " => " + ll.getTargetTitle());
                    cs.setField(ll.getTargetTitle(), ClickStreamTranslateTuple.TARGET_NAME_KEY);
                    return cs;
                });
                ;
        }

//        try {
//            translateInput.print();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        // Transform translateInput into normal input
        DataSet<ClickStreamTuple> input = translateInput.flatMap(new FlatMapFunction<ClickStreamTranslateTuple, ClickStreamTuple>() {
            @Override
            public void flatMap(ClickStreamTranslateTuple in, Collector<ClickStreamTuple> out) throws Exception {

                out.collect(new ClickStreamTuple(
                            in.getArticleName(), //referrerName,
                            in.getArticleId(), //referrerId,
                            0,
                            ClickStreamDataSetReader.getOutMap(in.getTargetName(), in.getClicks()),
                            ClickStreamDataSetReader.getOutMap(in.getTargetName(), in.getTargetId())
                    ));

                // Impressions
                if (in.getClicks() > 0)
                    out.collect(new ClickStreamTuple(in.getTargetName(), in.getTargetId(), in.getClicks(), new HashMap<>(), new HashMap<>()));
            }
        });

//        try {
//            input.print();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        // Group and reduce click streams
        return input
                .groupBy(0)
                .reduce(new ReduceFunction<ClickStreamTuple>() {
                    @Override
                    public ClickStreamTuple reduce(ClickStreamTuple a, ClickStreamTuple b) throws Exception {

                        a.getOutClicks().putAll(b.getOutClicks());
                        a.getOutIds().putAll(b.getOutIds());

                        return new ClickStreamTuple(
                                a.getArticleName(),
                                a.getArticleId(),
                                a.getImpressions() + b.getImpressions(),
                                a.getOutClicks(),
                                a.getOutIds());
                    }
                })
                ;
    }

    /**
     * Helper methods that enables reading from multiple inputs
     *
     * @param env
     * @param filename Separate multiple inputs by comma
     * @return
     */
    private static DataSet<ClickStreamTranslateTuple> readClickStreamDataSetInputs(ExecutionEnvironment env, String filename) {
        // Read input(s)
        DataSet<ClickStreamTranslateTuple> input = null;
        for(String f: filename.split(",")) {
            // Read current input
            DataSet<ClickStreamTranslateTuple> currentInput = env.readTextFile(f)
                    .flatMap(new ClickStreamDataSetReader());
            if(input == null) {
                // Set if it is first input
                input = currentInput;
            } else {
                // Otherwise union with previous inputs
                input = input.union(currentInput);
            }
        }
        return input;
    }

}
