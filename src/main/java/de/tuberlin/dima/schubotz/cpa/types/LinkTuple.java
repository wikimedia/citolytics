package de.tuberlin.dima.schubotz.cpa.types;

import eu.stratosphere.types.Pair;
import eu.stratosphere.types.StringValue;

/**
 * Created by Moritz on 27.04.14.
 */
public class LinkTuple extends Pair<StringValue, StringValue> {
    @Override
    public String toString() {
        return String.valueOf(getFirst()) + ";" + getSecond();
    }
}
