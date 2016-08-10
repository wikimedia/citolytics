package org.wikipedia.citolytics.histogram;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.operators.DocumentProcessor;
import org.wikipedia.citolytics.cpa.types.WikiDocument;


public class HistogramMapper implements FlatMapFunction<String, HistogramResult> {
    @Override
    public void flatMap(String content, Collector<HistogramResult> resultCollector) throws Exception {

        WikiDocument doc = new DocumentProcessor().processDoc(content);

        if (doc == null) return;

        int linksCount = doc.getOutLinks().size();

        resultCollector.collect(new HistogramResult(doc.getNS(), 1, linksCount, Long.valueOf(linksCount * linksCount)));
    }
}
