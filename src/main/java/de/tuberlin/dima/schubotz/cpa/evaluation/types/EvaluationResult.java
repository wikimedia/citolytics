package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import de.tuberlin.dima.schubotz.cpa.types.StringListValue;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * article | list target
 */
public class EvaluationResult extends Tuple2<String, StringListValue> {
    public EvaluationResult() {

    }

    public EvaluationResult(String article, StringListValue targets) {
        setField(article, 0);
        setField(targets, 1);
    }
}
