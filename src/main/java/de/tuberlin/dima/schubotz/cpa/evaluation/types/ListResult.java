package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import de.tuberlin.dima.schubotz.cpa.types.list.StringListValue;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * article | list target | length
 */
public class ListResult extends Tuple3<String, StringListValue, Integer> {
    public ListResult() {

    }

    public ListResult(String article, StringListValue targets, int length) {
        setField(article, 0);
        setField(targets, 1);
        setField(length, 2);
    }
}
