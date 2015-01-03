package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * article | list target | length
 */
public class EvaluationResult extends Tuple3<String, String[], Integer> {
    public EvaluationResult() {

    }

    public EvaluationResult(String article, String[] targets, int length) {
        setField(article, 0);
        setField(targets, 1);
        setField(length, 2);
    }
}
