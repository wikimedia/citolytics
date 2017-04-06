package org.wikipedia.citolytics.cpa.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.LinkTuple;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import java.util.Map;

import static java.lang.Math.abs;
import static java.lang.Math.max;

/**
 * Processes Wikipedia documents with DocumentProcessor and extracts link pairs that are used for CPA computations.
 */
public class LinkPairExtractor extends RichFlatMapFunction<String, WikiSimResult> {

    private DocumentProcessor dp;

    private double[] alphas = new double[]{1.0};
    private boolean enableWiki2006 = false; // WikiDump of 2006 does not contain namespace tags
    private Configuration config;

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);

        config = parameter;
        enableWiki2006 = parameter.getBoolean("wiki2006", true);

        String[] arr = parameter.getString("alpha", "1.0").split(",");
        alphas = new double[arr.length];
        for (int i = 0; i < arr.length; i++) {
            alphas[i] = Double.parseDouble(arr[i]);
        }
    }

    private DocumentProcessor getDocumentProcessor() {
        if(dp == null) {
            dp = new DocumentProcessor();

            if(enableWiki2006) {
                dp.enableWiki2006();
            }
            dp.setInfoBoxRemoval(config.getBoolean("removeInfoBox", true));
        }
        return dp;
    }

    @Override
    public void flatMap(String content, Collector<WikiSimResult> out) throws Exception {
        DocumentProcessor dp = getDocumentProcessor();

        WikiDocument doc = dp.processDoc(content);

        if (doc == null) return;

        collectLinkPairs(doc, out);
    }

    private void collectLinkPairs(WikiDocument doc, Collector<WikiSimResult> out) {
        //Skip all namespaces other than main
        if (doc.getNS() != 0) {
            return;
        }

        // Loop all link pairs
        for (Map.Entry<String, Integer> outLink1 : doc.getOutLinks()) {
            for (Map.Entry<String, Integer> outLink2 : doc.getOutLinks()) {
                // Check alphabetical order (A before B)
                String pageA = outLink1.getKey();
                String pageB = outLink2.getKey();
                int order = pageA.compareTo(pageB);

                if (order < 0) {
                    int w1 = doc.getWordMap().floorEntry(outLink1.getValue()).getValue();
                    int w2 = doc.getWordMap().floorEntry(outLink2.getValue()).getValue();
                    int d = max(abs(w1 - w2), 1); // CPI definition

                    //recDistance.setValue(1 / (pow(d, α)));

                    if (LinkTuple.isValid(pageA, pageB)) {
                        out.collect(new WikiSimResult(pageA, pageB, d, alphas));
                    }

                }
            }
        }
    }
}
