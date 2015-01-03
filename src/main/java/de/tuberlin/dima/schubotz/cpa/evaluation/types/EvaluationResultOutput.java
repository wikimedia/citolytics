package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import org.apache.flink.api.java.tuple.Tuple18;

import java.util.Arrays;

/**
 * Nice result output, Arrays are printable (toString)
 */
public class EvaluationResultOutput extends Tuple18<String, Integer, String,
        String, Integer, Integer, Integer, Integer,
        String, Integer, Integer, Integer, Integer,
        String, Integer, Integer, Integer, Integer> {

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
