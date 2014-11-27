package de.tuberlin.dima.schubotz.cpa.contracts;

import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import de.tuberlin.dima.schubotz.cpa.types.DataTypes.HistogramResult;

import java.util.regex.Matcher;


public class HistogramMapper implements FlatMapFunction<String, HistogramResult> {
    @Override
    public void flatMap(String content, Collector<HistogramResult> resultCollector) throws Exception {
        // search for a page-xml entity
        Matcher m = DocumentProcessor.getPageMatcher(content);
        // if the record does not contain parsable page-xml
        if (!m.find()) return;

        // otherwise create a WikiDocument object from the xml
        WikiDocument doc = new WikiDocument();
        doc.setId(Integer.parseInt(m.group(3)));
        doc.setTitle(m.group(1));
        doc.setNS(Integer.parseInt(m.group(2)));
        doc.setText(StringUtils.unescapeEntities(m.group(4)));

        // skip docs from namespaces other than
        if (doc.getNS() != 0) return;

        int linksCount = doc.getOutLinks().size();

        //System.out.println(linksCount);

        resultCollector.collect(new HistogramResult(doc.getNS(), 1, linksCount, Long.valueOf(linksCount * linksCount)));
    }
}
