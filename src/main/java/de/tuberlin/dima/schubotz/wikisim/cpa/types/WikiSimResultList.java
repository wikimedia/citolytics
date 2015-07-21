package de.tuberlin.dima.schubotz.wikisim.cpa.types;

import org.apache.flink.types.IntValue;
import org.apache.flink.types.ListValue;

/**
 * Created by malteschwarzer on 28.11.14.
 */
public class WikiSimResultList extends ListValue<IntValue> {

    public String toString() {
        // removes brackets
        return super.toString().substring(1, super.toString().length() - 1);
    }
}
