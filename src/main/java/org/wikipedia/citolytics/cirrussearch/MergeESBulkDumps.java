package org.wikipedia.citolytics.cirrussearch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;

import java.util.regex.Pattern;

/**
 * Convert ESBulk (two lines) into delimited ESBulk (single line):
 *
 * $ sed 'N;s/\n/|/' cirrus.json > cirrus.single.json
 * $ sed 'N;s/\n/|/' citolytics.json > citolytics.single.json
 *
 * $ flink run MergeESBulkDumps --cirrus cirrus.single.json --citolytics citolytics.single.json --output $OUTPUT_DIR/merged.json
 *
 * $ hdfs dfs -getmerge $OUTPUT_DIR/merged.json tmp/${WIKI}_merged.json
 *
 */
public class MergeESBulkDumps extends WikiSimAbstractJob<Tuple1<String>> {
    public static void main(String[] args) throws Exception {
        new MergeESBulkDumps().start(args);
    }

    @Override
    public void plan() throws Exception {

        outputFilename = getParams().getRequired("output");
        String cirrusPath = getParams().getRequired("cirrus");
        String citolyticsPath = getParams().getRequired("citolytics");

        DataSet<Tuple2<String, String>> cirrusSet = env.readTextFile(cirrusPath).map(new ESBulkReader());
        DataSet<Tuple2<String, String>> citolyticsSet = env.readTextFile(citolyticsPath).map(new ESBulkReader());

        result = cirrusSet.leftOuterJoin(citolyticsSet)
            .where(0)
            .equalTo(0)
            .with(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple1<String>>() {
                @Override
                public Tuple1<String> join(Tuple2<String, String> a, Tuple2<String, String> b) throws Exception {
                    String action = "{\"index\":{\"_type\":\"page\",\"_id\":\"" + a.f0 + "\"}}\n";

                    if(b == null) {
                        // Use only index
                        return new Tuple1<>(action + a.f1);
                    } else if(a == null) {
                        throw new Exception("Both sides of join are null.");
                    } else {

                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode indexDoc = mapper.readTree(a.f1);

                        ObjectMapper updateMapper = new ObjectMapper();
                        JsonNode updateDoc = updateMapper.readTree(b.f1);

                        ObjectNode docNode = (ObjectNode) indexDoc;

                        docNode.put("citolytics", updateDoc.get("doc").get("citolytics"));

                        return new Tuple1<>(action + docNode.toString());
                    }
                }
            });

    }

    class ESBulkReader implements MapFunction<String, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> map(String s) throws Exception {
            String[] cols = Pattern.compile(Pattern.quote(WikiSimConfiguration.csvFieldDelimiter)).split(s, 2);

            if(cols.length != 2)
                throw new Exception("Invalid input line (length=" + cols.length +"): " + s);

            String actionStr = cols[0];
//            String doc = cols[1];

            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode action = mapper.readTree(actionStr);
                //JsonNode doc = mapper.readTree(cols[1]);
                String docId;

                if (action.has("update") && action.get("update").has("_id")) {
                    docId = action.get("update").get("_id").getTextValue();

                    if(docId == null)
                        docId = action.get("update").get("_id").toString();

                } else if (action.has("index") && action.get("index").has("_id")) {
                    docId = action.get("index").get("_id").getTextValue();

                } else {
                    throw new Exception("Unsupported action: " + cols[0]);
                }

                if(docId == null) {
                    throw new Exception("Invalid doc Id: " + actionStr + "// " + action.get("update").get("_id").toString());
                }

                return new Tuple2<>(docId, cols[1]);
            } catch (JsonParseException e) {
                throw new Exception("Cannot parse JSON: " + cols[0] + " // " + e.getMessage());
            }
        }
    }
}
