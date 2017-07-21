package org.wikipedia.citolytics.seealso.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.wikipedia.citolytics.seealso.types.SeeAlsoLinks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Pattern;

public class SeeAlsoInputMapper implements MapFunction<String, SeeAlsoLinks> {
    @Override
    public SeeAlsoLinks map(String s) throws Exception {
        String[] cols = s.split(Pattern.quote("|"));
        if (cols.length >= 2) {
            String[] links = cols[1].split(Pattern.quote("#"));

            return new SeeAlsoLinks(cols[0], new HashSet<>(Arrays.asList(links)));
        } else {
            throw new Exception("Cannot read See also input: " + s);
        }
    }
}
