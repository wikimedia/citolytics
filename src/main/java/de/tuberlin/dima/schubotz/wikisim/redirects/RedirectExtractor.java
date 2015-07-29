package de.tuberlin.dima.schubotz.wikisim.redirects;

import de.tuberlin.dima.schubotz.wikisim.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.wikisim.cpa.io.WikiOutputFormat;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.wikisim.cpa.utils.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts all redirects from Wikipedia XML Dump. Redirects are taken from <redirect>-tag.
 * <p/>
 * Output CSV: Source | Target
 */
public class RedirectExtractor {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length < 2) {
            System.err.println("Input/output parameters missing!");
            System.err.println("Arguments: [WIKISET] [OUTPUT-LIST] [OUTPUT-STATS]");
            System.exit(1);
        }

        String inputWikiSet = args[0];
        String outputListFilename = args[1];

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputWikiSet);

        DataSet<Tuple2<String, String>> res = text.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String content, Collector<Tuple2<String, String>> out) throws Exception {
                Pattern pattern = Pattern.compile("(?:<page>\\s+)(?:<title>)(.*?)(?:</title>)\\s+(?:<ns>)(.*?)(?:</ns>)\\s+(?:<id>)(.*?)(?:</id>)(?:.*?)(?:<text.*?>)(.*?)(?:</text>)", Pattern.DOTALL);

                Matcher m = pattern.matcher(content);
                // if the record does not contain parsable page-xml
                if (!m.find()) return;

                // otherwise create a WikiDocument object from the xml
                WikiDocument doc = new WikiDocument();

                doc.setId(Integer.parseInt(m.group(3)));
                doc.setTitle(StringUtils.unescapeEntities(m.group(1)));
                doc.setNS(Integer.parseInt(m.group(2)));

                if (doc.getNS() != 0) return;

                Pattern redirect = Pattern.compile("<redirect title=\"(.+?)\"", Pattern.CASE_INSENSITIVE);
                Matcher mr = redirect.matcher(content);

                if (!mr.find()) return;

                out.collect(new Tuple2<>(
                        doc.getTitle(),
                        StringUtils.unescapeEntities(mr.group(1))
                ));
            }
        });


        if (outputListFilename.equals("print")) {
            res.print();
        } else {
            res.write(new WikiOutputFormat<Tuple2<String, String>>(outputListFilename), outputListFilename, FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1);
            env.execute("RedirectionExtractor");
        }

    }
}
