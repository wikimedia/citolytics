package org.wikipedia.citolytics.redirects.single;

import org.apache.flink.api.java.tuple.Tuple4;

import java.util.regex.Pattern;

/**
 * RecommendationPair as strings
 * 0: Hash
 * 1: Page A
 * 2: Page B
 * 3: Everything else
 */
public class WikiSimRedirectsResult extends Tuple4<Long, String, String, String> {
    public static final String delimiterPattern = Pattern.quote("|");

    public WikiSimRedirectsResult() {
        // Flink needs empty constructor
    }

    public WikiSimRedirectsResult(String delimitedLine) {
        String[] cols = delimitedLine.split(delimiterPattern, getArity());

        setField(Long.valueOf(cols[0]), 0);
        setField(cols[1], 1);
        setField(cols[2], 2);
        setField(cols[3], 3); // rest of RecommendationPair
    }

}
