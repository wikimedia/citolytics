package de.tuberlin.dima.schubotz.cpa.types;

import org.apache.flink.types.Pair;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

/**
 * Created by Moritz on 27.04.14.
 */

@Deprecated
public class LinkTupleOld extends Pair<StringValue, StringValue> {

    @Override
    public String toString() {
        return String.valueOf(getFirst()) + ";" + getSecond();
    }

}
/*
public class LinkTuple {
    private String first;
    private String second;

    public LinkTuple() {

    }

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    public void setSecond(StringValue second) {
        this.second = second.getValue();
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public void setFirst(StringValue first) {
        this.first = first.getValue();
    }
}*/