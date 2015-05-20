package de.tuberlin.dima.schubotz.cpa.redirects;

import de.tuberlin.dima.schubotz.cpa.types.list.DoubleListValue;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.types.DoubleValue;

import java.util.regex.Pattern;

public class WikiSimRedirectResult extends Tuple6<
        Long, // hash
        String,
        String,
        Long, // distance
        Integer, // count
//        Long, // distSquared
//        Long, // min
//        Long, // max
        DoubleListValue // CPA
        > {

    public static final String delimiterPattern = Pattern.quote("|");
    public WikiSimRedirectResult() {

    }

    public WikiSimRedirectResult(String delimitedLine) {
        String[] cols = delimitedLine.split(delimiterPattern, 6);

        setField(Long.valueOf(cols[0]), 0);
        setField(cols[1], 1);
        setField(cols[2], 2);
        setField(Long.valueOf(cols[3]), 3);
        setField(Integer.valueOf(cols[4]), 4);

        // DoubleListValue
        String[] dbs = cols[5].split(delimiterPattern);
        DoubleListValue list = new DoubleListValue();
        for (String db : dbs) {
            list.add(new DoubleValue(Double.valueOf(db)));
        }
        setField(list, 5);
    }

    public void sumWith(WikiSimRedirectResult otherResult) throws Exception {
        setField(((Long) otherResult.getField(3)) + f3, 3); // distance
        setField(((Integer) otherResult.getField(4)) + f4, 4); // count
        setField(DoubleListValue.sum((DoubleListValue) otherResult.getField(5), f5), 5);
    }
}
