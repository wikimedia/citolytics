package de.tuberlin.dima.schubotz.cpa.redirects;

import de.tuberlin.dima.schubotz.cpa.types.list.DoubleListValue;
import org.apache.flink.api.java.tuple.Tuple6;

import java.util.ArrayList;
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

        // ArrayList
        //setField(StringUtils.getDoubleListFromString(cols[5], delimiterPattern), 5);

        // DoubleListValue
        setField(DoubleListValue.valueOf(cols[5], delimiterPattern), 5);
    }


    public void sumWith(WikiSimRedirectResult otherResult) throws Exception {
        setField(((Long) otherResult.getField(3)) + f3, 3); // distance
        setField(((Integer) otherResult.getField(4)) + f4, 4); // count

        setField(DoubleListValue.sum(f5, (DoubleListValue) otherResult.getField(5)), 5);
//        setField(sum((ArrayList<Double>) otherResult.getField(5), f5), 5);
    }

    dd

    public static ArrayList<Double> sum(ArrayList<Double> firstList, ArrayList<Double> secondList) throws Exception {
        if (firstList.size() != secondList.size()) {
            throw new Exception("Cannot sum lists with different size.");
        }

        int i = 0;
        for (double firstValue : firstList) {
            firstList.set(i, firstValue + secondList.get(i));
            i++;
        }

        return firstList;
    }
}
