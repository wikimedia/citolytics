package de.tuberlin.dima.schubotz.wikisim.seealso.types;

import de.tuberlin.dima.schubotz.wikisim.cpa.types.list.IntegerListValue;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.list.StringListValue;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.types.IntValue;

/**
 * Evaluation result (output)
 */
public class EvaluationResult extends Tuple18<String, Integer, StringListValue,
        StringListValue, Integer, Double, Double, IntegerListValue,
        StringListValue, Integer, Double, Double, IntegerListValue,
        StringListValue, Integer, Double, Double, IntegerListValue> {
    public final static StringListValue EMPTY_LIST = new StringListValue();

    public static int topKlength;

    public final static int SEEALSO_LIST_KEY = 2;
    public final static int CPA_LIST_KEY = 3;
    public final static int CPA_HRR_KEY = 5;
    public final static int CPA_TOPK_KEY = 6;
    public final static int CPA_MATCHES_KEY = 7;
    public final static int COCIT_LIST_KEY = 8;
    public final static int COCIT_HRR_KEY = 10;
    public final static int COCIT_TOPK_KEY = 11;
    public final static int COCIT_MATCHES_KEY = 12;
    public final static int MLT_LIST_KEY = 13;
    public final static int MLT_HRR_KEY = 15;
    public final static int MLT_TOPK_KEY = 16;
    public final static int MLT_MATCHES_KEY = 17;


    public EvaluationResult() {

    }

    public EvaluationResult(String key, String[] seeAlsoArray) {
        init(key, StringListValue.valueOf(seeAlsoArray));
    }

    public EvaluationResult(String key, StringListValue seeAlsoList) {
        init(key, seeAlsoList);
    }

    public void init(String key, StringListValue seeAlsoList) {
        setField(key, 0);
        setField(seeAlsoList.size(), 1);
        setField(seeAlsoList, 2);

        // CPA
        setField(EMPTY_LIST, CPA_LIST_KEY);
        setField(0, CPA_LIST_KEY + 1); // list size

        // CoCit
        setField(EMPTY_LIST, COCIT_LIST_KEY);
        setField(0, COCIT_LIST_KEY + 1);

        // MLT
        setField(EMPTY_LIST, MLT_LIST_KEY);
        setField(0, MLT_LIST_KEY + 1);

        // Double fields
        for (int doubleField : new int[]{CPA_HRR_KEY, CPA_TOPK_KEY, COCIT_HRR_KEY, COCIT_TOPK_KEY, MLT_HRR_KEY, MLT_TOPK_KEY}) {
            setField(new Double(0), doubleField);
        }

        initMatchesCount();
    }

    public void initMatchesCount() {
        IntegerListValue emptyList = new IntegerListValue();

        for (int i = 0; i < topKlength; i++) {
            emptyList.add(new IntValue(0));
        }

        setField(emptyList, CPA_MATCHES_KEY);
        setField(emptyList, MLT_MATCHES_KEY);
        setField(emptyList, COCIT_MATCHES_KEY);

    }

    public void setMatchesCount(int count, int key, int i) {
        try {
            ((IntegerListValue) this.getField(key)).set(i, new IntValue(count));
        } catch (IndexOutOfBoundsException e) {
            ((IntegerListValue) this.getField(key)).add(i, new IntValue(count));
        }
    }

    public void aggregateField(EvaluationResult a, EvaluationResult b, int key) {
        this.setField((int) a.getField(key) + (int) b.getField(key), key);
    }

    public void cocitCheck() {
        if (getField(CPA_LIST_KEY).equals(getField(COCIT_LIST_KEY)) == false) {
            System.out.println("### COCIT != CPA" + this.toString());
        }
    }
}
