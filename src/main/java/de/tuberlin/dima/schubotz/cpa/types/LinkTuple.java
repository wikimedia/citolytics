package de.tuberlin.dima.schubotz.cpa.types;

import org.apache.flink.types.Pair;
import org.apache.flink.types.StringValue;

/**
 * Created by Moritz on 27.04.14.
 */
public class LinkTuple extends Pair<StringValue, StringValue> {

    @Override
    public String toString() {
        return String.valueOf(getFirst()) + ";" + getSecond();
    }

}
