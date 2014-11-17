package de.tuberlin.dima.schubotz.cpa;

import de.tuberlin.dima.schubotz.cpa.contracts.DocumentProcessor;
import de.tuberlin.dima.schubotz.cpa.contracts.HistogramMapper;
import de.tuberlin.dima.schubotz.cpa.contracts.HistogramReducer;
import de.tuberlin.dima.schubotz.cpa.io.WikiDocumentDelimitedInputFormat;
import de.tuberlin.dima.schubotz.cpa.types.DataTypes;
import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts "See also" links from Wikipedia articles and creates CSV for DB import
 * <p/>
 * TODO: Check if link exists in article body
 * <p/>
 * table structure: article (primary key), "see also"-link target, position, total count of "see also"-links,
 */
public class SeeAlsoExtractor {

    public static String csvRowDelimiter = "\n";
    public static String csvFieldDelimiter = "\t";

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println(new WikiSim().getDescription());
            System.exit(1);
        }

        String inputFilename = args[0];
        String outputFilename = args[1];

        Configuration config = new Configuration();

        DataSource<String> text = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        // ArticleCounter, Links (, AvgDistance
        DataSet<Tuple4<String, String, Integer, Integer>> output = text.flatMap(new FlatMapFunction<String, Tuple4<String, String, Integer, Integer>>() {
            public void flatMap(String content, Collector out) {

                Matcher m = DocumentProcessor.getPageMatcher(content);
                // if the record does not contain parsable page-xml
                if (!m.find()) return;

                // otherwise create a WikiDocument object from the xml
                WikiDocument doc = new WikiDocument();
                doc.setId(Integer.parseInt(m.group(3)));
                doc.setTitle(m.group(1));
                doc.setNS(Integer.parseInt(m.group(2)));

                // skip docs from namespaces other than
                if (doc.getNS() != 0) return;

                doc.setText(getSeeAlsoSection(StringUtils.unescapeEntities(m.group(4))));
                List<Map.Entry<String, Integer>> links = doc.getOutLinks();

                for (Map.Entry<String, Integer> outLink : links) {

                    out.collect(new Tuple4<>(doc.getTitle(), outLink.getKey(), outLink.getValue(), links.size()));
                }
            }
        });

        output.writeAsCsv(outputFilename, csvRowDelimiter, csvFieldDelimiter, FileSystem.WriteMode.OVERWRITE);

        env.execute("WikiSeeAlsoExtractor");
    }


    /**
     * get text of "See Also" section
     *
     * @param wikiText
     * @return seeAlsoText
     */
    public static String getSeeAlsoSection(String wikiText) {
        int seeAlsoStart = -1;
        String seeAlsoText = "";
        String seeAlsoTitle = "==see also==";
        Pattern seeAlsoPattern = Pattern.compile(seeAlsoTitle, Pattern.CASE_INSENSITIVE);
        Matcher seeAlsoMatcher = seeAlsoPattern.matcher(wikiText);

        if (seeAlsoMatcher.find()) {
            seeAlsoStart = wikiText.indexOf(seeAlsoMatcher.group());
        }

        if (seeAlsoStart > 0) {
            int seeAlsoEnd = seeAlsoStart + seeAlsoTitle.length();
            int nextHeadlineStart = wikiText.substring(seeAlsoStart + seeAlsoTitle.length()).indexOf("==");

            if (nextHeadlineStart > 0) {
                seeAlsoText = wikiText.substring(seeAlsoStart, seeAlsoEnd + nextHeadlineStart);
            } else {
                seeAlsoText = wikiText.substring(seeAlsoStart);
            }
        }

        return seeAlsoText;
    }

}
