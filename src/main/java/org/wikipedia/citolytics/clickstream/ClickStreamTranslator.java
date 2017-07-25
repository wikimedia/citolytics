package org.wikipedia.citolytics.clickstream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.operators.ClickStreamDataSetReader;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;
import org.wikipedia.citolytics.clickstream.utils.ClickStreamHelper;

import java.util.Map;

/**
 * Translates click stream data set. Helper job to write intermediate results instead of on-the-fly translation.
 *
 * Format:
 * REF_ID_KEY, ARTICLE_ID_KEY, CLICKS_KEY, REF_NAME_KEY, ARTICLE_NAME_KEY, TYPE_KEY
 */
public class ClickStreamTranslator extends WikiSimAbstractJob<Tuple1<String>> {

    public static void main(String[] args) throws Exception {
        new ClickStreamTranslator().start(args);
    }

    @Override
    public void plan() throws Exception {
        setJobName("ClickStreamTranslator");

        outputFilename = getParams().getRequired("output");
        String inputPath = getParams().getRequired("input");
        String lang = getParams().getRequired("lang");
        String idTitleMappingFilename = getParams().get("id-title-mapping");
        String langLinksInputFilename = getParams().get("langlinks");

        // Load gold standard (include translations with requested, provide id-title-mapping if non-id format is used)
        result = ClickStreamHelper.getTranslatedClickStreamDataSet(env, inputPath, lang,
                        langLinksInputFilename, idTitleMappingFilename).flatMap(new FlatMapFunction<ClickStreamTuple, Tuple1<String>>() {
            @Override
            public void flatMap(ClickStreamTuple in, Collector<Tuple1<String>> out) throws Exception {
                for(Map.Entry<String, Integer> outClick: in.getOutClicks().entrySet()) {
                    String row = "";

                    row += in.getArticleId() + ClickStreamDataSetReader.DELIMITER;
                    row += in.getOutIds().get(outClick.getKey()) + ClickStreamDataSetReader.DELIMITER;
                    row += outClick.getValue() + ClickStreamDataSetReader.DELIMITER;
                    row += in.getArticleName() + ClickStreamDataSetReader.DELIMITER;
                    row += outClick.getKey() + ClickStreamDataSetReader.DELIMITER;
                    row += ClickStreamDataSetReader.getFilterType();

                    out.collect(new Tuple1<>(row));
                }
            }
        });
    }
}
