package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import de.tuberlin.dima.schubotz.cpa.types.list.IntegerListValue;
import org.apache.flink.api.java.tuple.Tuple12;

import java.util.Arrays;

/**
 * Nice result output, Arrays are printable (toString)
 */
public class EvaluationResultOutput extends Tuple12<String, Integer, String,
        String, Integer, IntegerListValue,
        String, Integer, IntegerListValue,
        String, Integer, IntegerListValue> {

    public EvaluationResultOutput() {

    }

    public EvaluationResultOutput(EvaluationFinalResult in) {
        for (int f = 0; f < getArity(); f++) {
            if (f == in.COCIT_LIST_KEY
                    || f == in.CPA_LIST_KEY
                    || f == in.MLT_LIST_KEY
                    || f == in.SEEALSO_LIST_KEY) {
                setField(Arrays.toString((String[]) in.getField(f)), f);
            } else {
                setField(in.getField(f), f);
            }
        }

    }
}
