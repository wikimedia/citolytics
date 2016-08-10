package org.wikipedia.citolytics.cpa.types;

import org.apache.flink.api.java.tuple.Tuple2;
import org.wikipedia.citolytics.cpa.utils.StringUtils;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;

public class LinkTuple extends Tuple2<String, String> {
    public LinkTuple() {

    }

    public LinkTuple(String first, String second) {
        setFirst(first);
        setSecond(second);
    }

    public void setFirst(String first) {
        setField(first.trim(), 0);
    }

    public void setSecond(String second) {
        setField(second.trim(), 1);
    }

    public String getFirst() {
        return getField(0);
    }

    public String getSecond() {
        return getField(1);
    }

    public boolean isValid() {
        return getFirst().length() > 0 && getSecond().length() > 0 && !getFirst().equals("\\") && !getSecond().equals("\\");
    }

    public static boolean isValid(String first, String second) {
        return first.length() > 0 && second.length() > 0 && !first.equals("\\") && !second.equals("\\");
    }

    public LinkTuple getTwin() {
        return new LinkTuple(getSecond(), getFirst());
    }

    @Override
    public String toString() {
        return String.valueOf(getField(0))
                + WikiSimConfiguration.csvFieldDelimiter
                + String.valueOf(getField(1));
    }

    public long getHash() {
        return getHash(getFirst(), getSecond());
    }

    public static long getHash(String first, String second) {
        return StringUtils.hash(first + WikiSimConfiguration.csvFieldDelimiter + second);
    }


}
