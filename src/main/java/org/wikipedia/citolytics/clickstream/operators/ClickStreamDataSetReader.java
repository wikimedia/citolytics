package org.wikipedia.citolytics.clickstream.operators;

//import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.com.google.common.collect.Sets;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTranslateTuple;

import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Pattern;


public class ClickStreamDataSetReader implements FlatMapFunction<String, ClickStreamTranslateTuple> {
    private final static int WITH_IDS_LENGTH = 6;
    private final static int WITH_IDS_TYPE_KEY = 5;
    private final static int WITH_IDS_REF_NAME_KEY = 3;
    private final static int WITH_IDS_REF_ID_KEY = 0;
    private final static int WITH_IDS_ARTICLE_NAME_KEY = 4;
    private final static int WITH_IDS_ARTICLE_ID_KEY = 1;
    private final static int WITH_IDS_CLICKS_KEY = 2;

    private final static int WITHOUT_IDS_LENGTH = 4;
    private final static int WITHOUT_IDS_TYPE_KEY = 2;
    private final static int WITHOUT_IDS_REF_NAME_KEY = 0;
    private final static int WITHOUT_IDS_ARTICLE_NAME_KEY = 1;
    private final static int WITHOUT_IDS_CLICKS_KEY = 3;

    private HashSet<String> getFilterNameSpaces() {
        return Sets.newHashSet(
                "other-wikipedia", "other-empty", "other-internal", "other-google", "other-yahoo",
                "other-bing", "other-facebook", "other-twitter", "other-other"
        );
    }

    private String getFilterType() {
        return "link";
    }

    private void collectRow(String[] cols, Collector<ClickStreamTranslateTuple> out, int typeKey, int refNameKey,
                            int refIdKey, int articleNameKey, int articleIdKey, int clicksKey) throws Exception {
        // Skip if is title row or not link type
        if (!cols[typeKey].equals("link")) { // cols[articleNameKey].equals("curr") ||
            return;
        }

        // replace underscore
        String referrerName = cols[refNameKey].replace("_", " ");
        String articleName = cols[articleNameKey].replace("_", " ");

        try {
            int articleId = articleIdKey < 0 ? 0 : Integer.valueOf(cols[articleIdKey]);
            int clicks = cols[clicksKey].isEmpty() ? 0 : Integer.valueOf(cols[clicksKey]);

            if (getFilterType().equals(cols[typeKey]) && !getFilterNameSpaces().contains(referrerName)) {
                int referrerId = refIdKey < 0 ? 0 : Integer.valueOf(cols[refIdKey]);

                out.collect(new ClickStreamTranslateTuple(
                        referrerName,
                        referrerId,
                        0,
                        articleName,
                        articleId,
                        clicks
                ));
            }

        } catch (NumberFormatException e) {
            throw new Exception("Cannot read from click stream data set. Col = " + Arrays.toString(cols));
        }
    }

    @Override
    public void flatMap(String s, Collector<ClickStreamTranslateTuple> out) throws Exception {

        String[] cols = s.split(Pattern.quote("\t"));

        // Use different key for different data set formats
        if (cols.length == WITH_IDS_LENGTH) {
            collectRow(cols, out, WITH_IDS_TYPE_KEY, WITH_IDS_REF_NAME_KEY, WITH_IDS_REF_ID_KEY, WITH_IDS_ARTICLE_NAME_KEY,
                    WITH_IDS_ARTICLE_ID_KEY, WITH_IDS_CLICKS_KEY);

        } else if(cols.length == WITHOUT_IDS_LENGTH) {
            // Data set has not page ids
            collectRow(cols, out, WITHOUT_IDS_TYPE_KEY, WITHOUT_IDS_REF_NAME_KEY, -1, WITHOUT_IDS_ARTICLE_NAME_KEY,
                    -1, WITHOUT_IDS_CLICKS_KEY);
        } else {
            throw new Exception("Wrong column length: " + cols.length + " (expected " + WITH_IDS_LENGTH + ") - " + Arrays.toString(cols));
        }
    }

}
