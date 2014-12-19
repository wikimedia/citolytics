package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import de.tuberlin.dima.schubotz.cpa.types.StringListValue;
import org.apache.flink.api.java.tuple.*;

/**
 *
 */
public class EvaluationFinalResult extends Tuple15<String, Integer, StringListValue,
        StringListValue, Integer, Integer, Integer,
        StringListValue, Integer, Integer, Integer,
        StringListValue, Integer, Integer, Integer> {
    public final static StringListValue EMPTY_LIST = StringListValue.valueOf(new String[]{});

    public final static int SEEALSO_LIST_KEY = 2;
    public final static int CPA_LIST_KEY = 3;
    public final static int CPA_MATCHES_KEY = 4;
    public final static int COCIT_LIST_KEY = 7;
    public final static int COCIT_MATCHES_KEY = 8;
    public final static int MLT_LIST_KEY = 11;
    public final static int MLT_MATCHES_KEY = 12;


    public EvaluationFinalResult() {

    }

    public EvaluationFinalResult(String key, StringListValue seeAlsoList) {
        setField(key, 0);
        setField(seeAlsoList.size(), 1);
        setField(seeAlsoList, 2);

        // CPA
        setField(EMPTY_LIST, CPA_LIST_KEY);
        setField(0, CPA_MATCHES_KEY);
        setField(0, CPA_MATCHES_KEY + 1);
        setField(0, CPA_MATCHES_KEY + 2);


        // CoCit
        setField(EMPTY_LIST, COCIT_LIST_KEY);
        setField(0, COCIT_MATCHES_KEY);
        setField(0, COCIT_MATCHES_KEY + 1);
        setField(0, COCIT_MATCHES_KEY + 2);


        // MLT
        setField(EMPTY_LIST, MLT_LIST_KEY);
        setField(0, MLT_MATCHES_KEY);
        setField(0, MLT_MATCHES_KEY + 1);
        setField(0, MLT_MATCHES_KEY + 2);


    }

//    public String toString() {
//        return getField(0) + "; " + getField(CPA_MATCHES_KEY) + " ; " + getField(COCIT_MATCHES_KEY) + " ; " + getField(MLT_MATCHES_KEY);
//    }

    public void aggregateField(EvaluationFinalResult a, EvaluationFinalResult b, int key) {
        this.setField((int) a.getField(key) + (int) b.getField(key), key);
    }
}
