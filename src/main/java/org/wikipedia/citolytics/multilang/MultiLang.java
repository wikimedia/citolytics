package org.wikipedia.citolytics.multilang;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handling links between different Wikipedia languages
 *
 * 1. Read langlink SQL dump for enwiki
 * 2. Prepare locale result set (WikiSim, SeeAlso, ClickStreams, ...)
 * 3. Map all to en-wiki
 * 4. Recompute results
 */
public class MultiLang extends WikiSimAbstractJob<LangLinkTuple> {
    public static void main(String[] args) throws Exception {
        new MultiLang().start(args);
    }

    @Override
    public void plan() throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        outputFilename = params.getRequired("output");
        result = readLangLinksDataSet(env, params.getRequired("input"));

    }


    public static DataSet<LangLinkTuple> readLangLinksDataSet(ExecutionEnvironment env, String pathToDataSet, String lang) {
        return readLangLinksDataSet(env, pathToDataSet)
                .filter(new LangFilter(lang));
    }

    static class LangFilter implements FilterFunction<LangLinkTuple> {
        private String lang;

        public LangFilter(String lang) {
            this.lang = lang;
        }

        @Override
        public boolean filter(LangLinkTuple link) throws Exception {
            return link.getLang().equalsIgnoreCase(lang);
        }
    }

    public static DataSet<LangLinkTuple> readLangLinksDataSet(ExecutionEnvironment env, String pathToDataSet) {
        DataSource<String> sql = env.readFile(new LangLinksFormat(), pathToDataSet);

        // Quick & dirty SQL dump parser
        return sql.flatMap(new FlatMapFunction<String, LangLinkTuple>() {
            @Override
            public void flatMap(String s, Collector<LangLinkTuple> out) throws Exception {
                Pattern p = Pattern.compile("^\\(([0-9]+),'(.*?)','(.*?)'");
                Matcher m = p.matcher(s);
                if (m.find()) {
                    // Match VALUES
                    collectMatch(m, out);
                } else {
                    // Match INSERT INTO statement
                    Pattern p2 = Pattern.compile("([0-9]+),'(.*?)','(.*?)'$");
                    Matcher m2 = p2.matcher(s);

                    if (m2.find()) {
                        collectMatch(m2, out);
                    } else {
                        // Nothing match at all.. s is CREATE TABLE statement
                    }
                }
            }

            private void collectMatch(Matcher matcher, Collector<LangLinkTuple> out) {
                out.collect(new LangLinkTuple(Integer.parseInt(matcher.group(1)), matcher.group(2), matcher.group(3)));
            }
        });
    }

}
