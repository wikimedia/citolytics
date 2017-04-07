package org.wikipedia.citolytics.seealso.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

public class SeeAlsoInputMapper implements MapFunction<String, Tuple2<String, ArrayList<String>>> {
    @Override
    public Tuple2<String, ArrayList<String>> map(String s) throws Exception {
        String[] cols = s.split(Pattern.quote("|"));
        if (cols.length >= 2) {
            String[] links = cols[1].split(Pattern.quote("#"));

            return new Tuple2<>(cols[0], new ArrayList<>(Arrays.asList(links)));
        } else {
            throw new Exception("Cannot read See also input: " + s);
        }
    }
}
