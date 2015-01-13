package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import de.tuberlin.dima.schubotz.cpa.types.list.IntegerListValue;
import de.tuberlin.dima.schubotz.cpa.types.list.StringListValue;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.types.IntValue;

/**
 *
 */
public class EvaluationResult extends Tuple15<String, Integer, StringListValue,
        StringListValue, Integer, Double, IntegerListValue,
        StringListValue, Integer, Double, IntegerListValue,
        StringListValue, Integer, Double, IntegerListValue> {
    public final static StringListValue EMPTY_LIST = new StringListValue();

    public final static int SEEALSO_LIST_KEY = 2;
    public final static int CPA_LIST_KEY = 3;
    public final static int CPA_MRR_KEY = 5;
    public final static int CPA_MATCHES_KEY = 6;
    public final static int COCIT_LIST_KEY = 7;
    public final static int COCIT_MRR_KEY = 9;
    public final static int COCIT_MATCHES_KEY = 10;
    public final static int MLT_LIST_KEY = 11;
    public final static int MLT_MMR_KEY = 13;
    public final static int MLT_MATCHES_KEY = 14;


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
        setField(new Double(0), CPA_MRR_KEY);
        setField(new IntegerListValue(), CPA_MATCHES_KEY);
//        setField(0, CPA_MATCHES_KEY + 1);
//        setField(0, CPA_MATCHES_KEY + 2);


        // CoCit
        setField(EMPTY_LIST, COCIT_LIST_KEY);
        setField(0, COCIT_LIST_KEY + 1);
        setField(new IntegerListValue(), COCIT_MATCHES_KEY);
        setField(new Double(0), COCIT_MRR_KEY);
//        setField(0, COCIT_MATCHES_KEY + 1);
//        setField(0, COCIT_MATCHES_KEY + 2);


        // MLT
        setField(EMPTY_LIST, MLT_LIST_KEY);
        setField(0, MLT_LIST_KEY + 1);
        setField(new Double(0), MLT_MMR_KEY);
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

    public void aggregateField(EvaluationResult a, EvaluationResult b, int key) {
        this.setField((int) a.getField(key) + (int) b.getField(key), key);
    }

}
