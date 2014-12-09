package de.tuberlin.dima.schubotz.cpa.contracts;

import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import de.tuberlin.dima.schubotz.cpa.types.HistogramResult;

import java.util.regex.Matcher;


public class HistogramMapper implements FlatMapFunction<String, HistogramResult> {
    @Override
    public void flatMap(String content, Collector<HistogramResult> resultCollector) throws Exception {

        WikiDocument doc = DocumentProcessor.processDoc(content);

        if (doc == null) return;

        int linksCount = doc.getOutLinks().size();

        //System.out.println(linksCount);

        resultCollector.collect(new HistogramResult(doc.getNS(), 1, linksCount, Long.valueOf(linksCount * linksCount)));
    }
}
