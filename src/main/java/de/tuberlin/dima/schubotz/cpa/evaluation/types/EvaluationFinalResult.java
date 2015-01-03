package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import org.apache.flink.api.java.tuple.Tuple18;

/**
 *
 */
public class EvaluationFinalResult extends Tuple18<String, Integer, String[],
        String[], Integer, Integer, Integer, Integer,
        String[], Integer, Integer, Integer, Integer,
        String[], Integer, Integer, Integer, Integer> {
    public final static String[] EMPTY_LIST = new String[]{};

    public final static int SEEALSO_LIST_KEY = 2;
    public final static int CPA_LIST_KEY = 3;
    public final static int CPA_MATCHES_KEY = 5;
    public final static int COCIT_LIST_KEY = 8;
    public final static int COCIT_MATCHES_KEY = 10;
    public final static int MLT_LIST_KEY = 13;
    public final static int MLT_MATCHES_KEY = 15;


    public EvaluationFinalResult() {

    }

    public EvaluationFinalResult(String key, String[] seeAlsoList) {
        setField(key, 0);
        setField(seeAlsoList.length, 1);
        setField(seeAlsoList, 2);

        // CPA
        setField(EMPTY_LIST, CPA_LIST_KEY);
        setField(0, CPA_LIST_KEY + 1); // list size
        setField(0, CPA_MATCHES_KEY);
        setField(0, CPA_MATCHES_KEY + 1);
        setField(0, CPA_MATCHES_KEY + 2);


        // CoCit
        setField(EMPTY_LIST, COCIT_LIST_KEY);
        setField(0, COCIT_LIST_KEY + 1);
        setField(0, COCIT_MATCHES_KEY);
        setField(0, COCIT_MATCHES_KEY + 1);
        setField(0, COCIT_MATCHES_KEY + 2);


        // MLT
        setField(EMPTY_LIST, MLT_LIST_KEY);
        setField(0, MLT_LIST_KEY + 1);
        setField(0, MLT_MATCHES_KEY);
        setField(0, MLT_MATCHES_KEY + 1);
        setField(0, MLT_MATCHES_KEY + 2);


    }

    public void aggregateField(EvaluationFinalResult a, EvaluationFinalResult b, int key) {
        this.setField((int) a.getField(key) + (int) b.getField(key), key);
    }

}
