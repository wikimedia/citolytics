package de.tuberlin.dima.schubotz.cpa.histogram;

import de.tuberlin.dima.schubotz.cpa.contracts.DocumentProcessor;
import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;


public class HistogramMapper implements FlatMapFunction<String, HistogramResult> {
    @Override
    public void flatMap(String content, Collector<HistogramResult> resultCollector) throws Exception {

        WikiDocument doc = new DocumentProcessor().processDoc(content);

        if (doc == null) return;

        int linksCount = doc.getOutLinks().size();

        resultCollector.collect(new HistogramResult(doc.getNS(), 1, linksCount, Long.valueOf(linksCount * linksCount)));
    }
}
