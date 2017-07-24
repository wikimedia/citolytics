package org.wikipedia.citolytics.clickstream.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cirrussearch.IdTitleMappingExtractor;
import org.wikipedia.citolytics.clickstream.operators.ClickStreamDataSetReader;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTranslateTuple;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;
import org.wikipedia.citolytics.cpa.types.IdTitleMapping;
import org.wikipedia.citolytics.multilang.LangLinkTuple;
import org.wikipedia.citolytics.multilang.MultiLang;

import java.util.HashMap;
import java.util.Map;

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
    public static DataSet<ClickStreamTuple> getClickStreamDataSet(ExecutionEnvironment env, String filename) throws Exception {
        return getTranslatedClickStreamDataSet(env, filename, null, null, null);
    }

    /**
     * Get click stream data set, translates if requested and adds page ids if those are missing in input data
     *
     * @param env ExecutionEnvironment.
     * @param filename Path to data set (separate multiple files by comma)
     * @param lang Language code of used WikiSim input
     * @param langLinksFilename Path to enwiki language links
     * @param idTitleMappingFilename Path to id-title mapping of enwiki
     * @return
     * @throws Exception
     */
    public static DataSet<ClickStreamTuple> getTranslatedClickStreamDataSet(ExecutionEnvironment env, String filename,
                                                                            String lang, String langLinksFilename, String idTitleMappingFilename) throws Exception {
        DataSet<ClickStreamTranslateTuple> translateInput = readClickStreamDataSetInputs(env, filename);

        // Check for ids
        DataSet<ClickStreamTranslateTuple> translateInputWithIds = translateInput.filter(new FilterFunction<ClickStreamTranslateTuple>() {
            @Override
            public boolean filter(ClickStreamTranslateTuple t) throws Exception {
                return t.hasIds();
            }
        });

        // If id does not exist, use id-title mapping to get corresponding page ids
        if(idTitleMappingFilename != null) {
            DataSet<IdTitleMapping> idTitleMapping = IdTitleMappingExtractor.getIdTitleMapping(env, idTitleMappingFilename, null);

            // Filter for tuples without ids
            DataSet<ClickStreamTranslateTuple> translateInputWithoutIds = translateInput
                    .filter(new FilterFunction<ClickStreamTranslateTuple>() {
                        @Override
                        public boolean filter(ClickStreamTranslateTuple t) throws Exception {
                            return !t.hasIds();
                        }
                    })
                    // Get page id of source article
                    .join(idTitleMapping)
                    .where(ClickStreamTranslateTuple.ARTICLE_NAME_KEY)
                    .equalTo(IdTitleMapping.TITLE_KEY)
                    .with(new JoinFunction<ClickStreamTranslateTuple, IdTitleMapping, ClickStreamTranslateTuple>() {
                        @Override
                        public ClickStreamTranslateTuple join(ClickStreamTranslateTuple cs, IdTitleMapping mapping) throws Exception {
                            cs.setArticleId(mapping.getField(IdTitleMapping.ID_KEY));
                            return cs;
                        }
                    })
                    // get page id of target article
                    .join(idTitleMapping)
                    .where(ClickStreamTranslateTuple.TARGET_NAME_KEY)
                    .equalTo(IdTitleMapping.TITLE_KEY)
                    .with(new JoinFunction<ClickStreamTranslateTuple, IdTitleMapping, ClickStreamTranslateTuple>() {
                        @Override
                        public ClickStreamTranslateTuple join(ClickStreamTranslateTuple cs, IdTitleMapping mapping) throws Exception {
                            cs.setTargetId(mapping.getField(IdTitleMapping.ID_KEY));
                            return cs;
                        }
                    });

            // Merge again
            translateInput = translateInputWithIds.union(translateInputWithoutIds);
        } else {
            translateInput = translateInputWithIds;
        }

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
                    cs.setField(ll.getTargetTitle(), ClickStreamTranslateTuple.ARTICLE_NAME_KEY);
                    return cs;
                })
                // target name
                .join(langLinks)
                .where(ClickStreamTranslateTuple.TARGET_ID_KEY)
                .equalTo(LangLinkTuple.PAGE_ID_KEY)
                .with((JoinFunction<ClickStreamTranslateTuple, LangLinkTuple, ClickStreamTranslateTuple>) (cs, ll) -> {
                    // Replace names with translated values
                    cs.setField(ll.getTargetTitle(), ClickStreamTranslateTuple.TARGET_NAME_KEY);
                    return cs;
                });

        }

        // Transform translateInput into normal input
        DataSet<ClickStreamTuple> input = translateInput.flatMap(new FlatMapFunction<ClickStreamTranslateTuple, ClickStreamTuple>() {
            @Override
            public void flatMap(ClickStreamTranslateTuple in, Collector<ClickStreamTuple> out) throws Exception {
                ClickStreamTuple t = new ClickStreamTuple(
                        in.getArticleName(), //referrerName,
                        in.getArticleId(), //referrerId,
                        0,
                        getOutMap(in.getTargetName(), in.getClicks()),
                        getOutMap(in.getTargetName(), in.getTargetId())
                );
                out.collect(t);

                // Impressions
                if (in.getClicks() > 0) {
                    out.collect(new ClickStreamTuple(in.getTargetName(), in.getTargetId(), in.getClicks(), new HashMap<>(), new HashMap<>()));
                }
            }
        });

        // Group and reduce click streams
        return input
                .groupBy(0)
                .reduce(new ReduceFunction<ClickStreamTuple>() {
                    @Override
                    public ClickStreamTuple reduce(ClickStreamTuple a, ClickStreamTuple b) throws Exception {

                        // Merge out clicks
                        HashMap<String, Integer> outClicks = new HashMap<>();
                        outClicks.putAll(a.getOutClicks());

                        for(Map.Entry<String, Integer> bb: b.getOutClicks().entrySet()) {
                            if(outClicks.containsKey(bb.getKey())) {
                                outClicks.put(bb.getKey(), bb.getValue() + outClicks.get(bb.getKey()));
                            } else {
                                outClicks.put(bb.getKey(), bb.getValue());
                            }
                        }

                        // Leads to inconsistent results
                        //outClicks.putAll(b.getOutClicks());

                        // Out ids can be overwritten (check if valid?)
                        HashMap<String, Integer> outIds = new HashMap<>();
                        outIds.putAll(a.getOutIds());
                        outIds.putAll(b.getOutIds());

                        return new ClickStreamTuple(
                                a.getArticleName(),
                                a.getArticleId(),
                                a.getImpressions() + b.getImpressions(),
                                outClicks,
                                outIds);
                    }
                });
    }

    /**
     * Helper methods that enables reading from multiple inputs
     *
     * @param env
     * @param filename Separate multiple inputs by comma
     * @return
     */
    public static DataSet<ClickStreamTranslateTuple> readClickStreamDataSetInputs(ExecutionEnvironment env, String filename) {
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

    public static HashMap<String, Integer> getOutMap(String link, int clicks_or_id) {
        HashMap<String, Integer> res = new HashMap<>();
        res.put(link, clicks_or_id);
        return res;
    }
}
