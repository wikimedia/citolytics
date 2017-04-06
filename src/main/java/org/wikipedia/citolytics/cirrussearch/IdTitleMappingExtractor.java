package org.wikipedia.citolytics.cirrussearch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.IdTitleMapping;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import java.util.regex.Pattern;

/**
 * Extracts from a Wiki XML dump the mapping from page title to page id.
 *
 */
public class IdTitleMappingExtractor extends WikiSimAbstractJob<IdTitleMapping> {
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

    public static DataSet<IdTitleMapping> extractIdTitleMapping(ExecutionEnvironment env, String inputFilename) {
        return extractIdTitleMapping(env, env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename));
    }

    public static DataSet<IdTitleMapping> extractIdTitleMapping(ExecutionEnvironment env, DataSource<String> wikiDump) {
        return wikiDump.flatMap(new  FlatMapFunction<String, IdTitleMapping>() {
            @Override
            public void flatMap(String s, Collector<IdTitleMapping> out) throws Exception {
                DocumentProcessor dp = new DocumentProcessor();
                WikiDocument doc = dp.processDoc(s);

                if(doc != null) {
                    out.collect(new IdTitleMapping(doc.getId(), doc.getTitle()));
                }
            }
        });
    }

    public static DataSet<IdTitleMapping> getIdTitleMapping(ExecutionEnvironment env, String idTitleMappingFilename,
                                                            String wikiDumpInputFilename) throws Exception {
        if(idTitleMappingFilename != null) {
            return env.readTextFile(idTitleMappingFilename).flatMap(new FlatMapFunction<String, IdTitleMapping>() {
                @Override
                public void flatMap(String s, Collector<IdTitleMapping> out) throws Exception {
                    String[] cols = s.split(Pattern.quote("|"));
                    if (cols.length != 2) {
                        throw new Exception("Invalid id title mapping: " + s);
                    }

                    out.collect(new IdTitleMapping(Integer.valueOf(cols[0]), cols[1]));
                }
            });
        } else if(wikiDumpInputFilename != null) {
            return extractIdTitleMapping(env, wikiDumpInputFilename);
        } else {
            throw new Exception("Could not get IdTitleMapping. Either idTitleMappingFilename or wikiDumpInputFilename needs to be set.");
        }
    }

}
