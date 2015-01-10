package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import de.tuberlin.dima.schubotz.cpa.types.list.IntegerListValue;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.types.IntValue;

/**
 *
 */
public class EvaluationFinalResult extends Tuple12<String, Integer, String[],
        String[], Integer, IntegerListValue,
        String[], Integer, IntegerListValue,
        String[], Integer, IntegerListValue> {
    public final static String[] EMPTY_LIST = new String[]{};

    public final static int SEEALSO_LIST_KEY = 2;
    public final static int CPA_LIST_KEY = 3;
    public final static int CPA_MATCHES_KEY = 5;
    public final static int COCIT_LIST_KEY = 6;
    public final static int COCIT_MATCHES_KEY = 8;
    public final static int MLT_LIST_KEY = 9;
    public final static int MLT_MATCHES_KEY = 11;


    public EvaluationFinalResult() {

    }

    public EvaluationFinalResult(String key, String[] seeAlsoList) {
        setField(key, 0);
        setField(seeAlsoList.length, 1);
        setField(seeAlsoList, 2);

        // CPA
        setField(EMPTY_LIST, CPA_LIST_KEY);
        setField(0, CPA_LIST_KEY + 1); // list size
        setField(new IntegerListValue(), CPA_MATCHES_KEY);
//        setField(0, CPA_MATCHES_KEY + 1);
//        setField(0, CPA_MATCHES_KEY + 2);


        // CoCit
        setField(EMPTY_LIST, COCIT_LIST_KEY);
        setField(0, COCIT_LIST_KEY + 1);
        setField(new IntegerListValue(), COCIT_MATCHES_KEY);
//        setField(0, COCIT_MATCHES_KEY + 1);
//        setField(0, COCIT_MATCHES_KEY + 2);


        // MLT
        setField(EMPTY_LIST, MLT_LIST_KEY);
        setField(0, MLT_LIST_KEY + 1);
        setField(new IntegerListValue(), MLT_MATCHES_KEY);
//        setField(0, MLT_MATCHES_KEY + 1);
//        setField(0, MLT_MATCHES_KEY + 2);

    }

    public void setMatchesCount(int count, int key, int i) {
        try {
            ((IntegerListValue) this.getField(key)).set(i, new IntValue(count));
        } catch (IndexOutOfBoundsException e) {
            ((IntegerListValue) this.getField(key)).add(i, new IntValue(count));
        }
    }

    public void aggregateField(EvaluationFinalResult a, EvaluationFinalResult b, int key) {
        this.setField((int) a.getField(key) + (int) b.getField(key), key);
    }

}
