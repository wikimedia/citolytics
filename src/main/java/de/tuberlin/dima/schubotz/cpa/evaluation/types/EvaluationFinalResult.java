package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import de.tuberlin.dima.schubotz.cpa.types.StringListValue;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;

/**
 * TODO: add total see also count
 */
public class EvaluationFinalResult extends Tuple8<String, StringListValue, StringListValue, Integer, StringListValue, Integer, StringListValue, Integer> {
    public final static StringListValue EMPTY_LIST = StringListValue.valueOf(new String[]{});

    public final static int CPA_LIST_KEY = 2;
    public final static int CPA_MATCHES_KEY = 3;
    public final static int COCIT_LIST_KEY = 4;
    public final static int COCIT_MATCHES_KEY = 5;
    public final static int MLT_LIST_KEY = 6;
    public final static int MLT_MATCHES_KEY = 7;


    public EvaluationFinalResult() {

    }

    public EvaluationFinalResult(String key, StringListValue seeAlsoList) {
        setField(key, 0);
        setField(seeAlsoList, 1);

        // CPA
        setField(EMPTY_LIST, CPA_LIST_KEY);
        setField(0, CPA_MATCHES_KEY);

        // CoCit
        setField(EMPTY_LIST, COCIT_LIST_KEY);
        setField(0, COCIT_MATCHES_KEY);

        // MLT
        setField(EMPTY_LIST, MLT_LIST_KEY);
        setField(0, MLT_MATCHES_KEY);
    }

    public String toString() {
        return getField(0) + "; " + getField(3) + " ; " + getField(5) + " ; " + getField(7);
    }
}
