package org.wikipedia.citolytics.cirrussearch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.WikiDocument;
import org.wikipedia.processing.DocumentProcessor;

/**
 * Extracts from a Wiki XML dump the mapping from page title to page id.
 *
 */
public class IdTitleMappingExtractor extends WikiSimAbstractJob<Tuple2<Integer, String>> {
    public static void main(String[] args) throws Exception {
        new IdTitleMappingExtractor().start(args);
    }

    @Override
    public void plan() throws Exception {
        jobName = "IdTitleMappingExtractor";
        ParameterTool params = ParameterTool.fromArgs(args);
        outputFilename = params.get("output");

        result = extractIdTitleMapping(env, params.get("input"));
    }

    public static DataSet<Tuple2<Integer, String>> extractIdTitleMapping(ExecutionEnvironment env, String inputFilename) {
        return env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename)
                .flatMap(new  FlatMapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<Integer, String>> out) throws Exception {
                DocumentProcessor dp = new DocumentProcessor();
                WikiDocument doc = dp.processDoc(s);

                if(doc != null) {
                    out.collect(new Tuple2<>(doc.getId(), doc.getTitle()));
                }
            }
        });
    }
}
