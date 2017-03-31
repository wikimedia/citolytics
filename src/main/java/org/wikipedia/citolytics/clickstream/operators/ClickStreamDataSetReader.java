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

    private HashSet<String> getFilterNameSpaces() {
        return Sets.newHashSet(
                "other-wikipedia", "other-empty", "other-internal", "other-google", "other-yahoo",
                "other-bing", "other-facebook", "other-twitter", "other-other"
        );
    }

    private String getFilterType() {
        return "link";
    }

    @Override
    public void flatMap(String s, Collector<ClickStreamTranslateTuple> out) throws Exception {
        int expectedCols = 6;

        String[] cols = s.split(Pattern.quote("\t"));
        if (cols.length == expectedCols) {
            // Skip if is title row or not link type
            if (cols[1].equals("prev_id") || !cols[5].equals("link")) {
                return;
            }

            // replace underscore
            String referrerName = cols[3].replace("_", " ");
            String currentName = cols[4].replace("_", " ");

            try {
                int currentId = Integer.valueOf(cols[1]);
                int clicks = cols[2].isEmpty() ? 0 : Integer.valueOf(cols[2]);

                if (getFilterType().equals(cols[5]) && !getFilterNameSpaces().contains(referrerName)) {
                    int referrerId = Integer.valueOf(cols[0]);

                    out.collect(new ClickStreamTranslateTuple(
                            referrerName,
                            referrerId,
                            0,
                            currentName,
                            currentId,
                            clicks
                    ));
                }

            } catch (NumberFormatException e) {
                throw new Exception("Cannot read from click stream data set. Col = " + s);
            }

        } else {
            throw new Exception("Wrong column length: " + cols.length + " (expected " + expectedCols + ") - " + Arrays.toString(cols));
        }
    }

}
