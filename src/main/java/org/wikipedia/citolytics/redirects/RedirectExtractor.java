package org.wikipedia.citolytics.redirects;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;
import org.wikipedia.citolytics.cpa.utils.WikiSimStringUtils;
import org.wikipedia.processing.types.WikiDocument;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts all redirects from Wikipedia XML Dump. Redirects are taken from <redirect>-tag.
 *
 * Arguments: --input <wiki-xml-dump> --output <out-csv>
 *
 * Output CSV: Source | Target
 *
 * TODO There is already an extra redirect data set available:
 * https://dumps.wikimedia.org/enwiki/20160901/enwiki-20160901-redirect.sql.gz
 * (redirects.sql dump cannot be used, since it has id->name mappings. We need name->name.)
 */
public class RedirectExtractor extends WikiSimAbstractJob<RedirectMapping> {

    private final static String REGEX = "(?:<page>\\s+)(?:<title>)(.*?)(?:</title>)\\s+(?:<ns>)(.*?)(?:</ns>)\\s+(?:<id>)(.*?)(?:</id>)(?:.*?)(?:<text.*?>)(.*?)(?:</text>)";

    public static void main(String[] args) throws Exception {
        new RedirectExtractor().start(args);
    }

    public void plan() {
        ParameterTool params = ParameterTool.fromArgs(args);

        outputFilename = params.getRequired("output");
        result = extractRedirectMappings(env, params.getRequired("input"));
    }

    public static DataSet<RedirectMapping> extractRedirectMappings(ExecutionEnvironment env, String wikiDumpInputFilename) {
        return extractRedirectMappings(env, env.readFile(new WikiDocumentDelimitedInputFormat(), wikiDumpInputFilename));
    }

    public static DataSet<RedirectMapping> extractRedirectMappings(ExecutionEnvironment env, DataSource<String> wikiDump) {
        return wikiDump.flatMap(new FlatMapFunction<String, RedirectMapping>() {
            @Override
            public void flatMap(String content, Collector<RedirectMapping> out) throws Exception {
                Pattern pattern = Pattern.compile(REGEX, Pattern.DOTALL);

                Matcher m = pattern.matcher(content);
                // if the record does not contain parsable page-xml
                if (!m.find()) return;

                // otherwise create a WikiDocument object from the xml
                WikiDocument doc = new WikiDocument();

                doc.setId(Integer.parseInt(m.group(3)));
                doc.setTitle(WikiSimStringUtils.unescapeEntities(m.group(1)));
                doc.setNS(Integer.parseInt(m.group(2)));

                if (doc.getNS() != 0) return;

                Pattern redirect = Pattern.compile("<redirect title=\"(.+?)\"", Pattern.CASE_INSENSITIVE);
                Matcher mr = redirect.matcher(content);

                if (!mr.find()) return;

                out.collect(new RedirectMapping(
                        doc.getTitle(),
                        WikiSimStringUtils.unescapeEntities(mr.group(1))
                ));
            }
        });
    }
}
