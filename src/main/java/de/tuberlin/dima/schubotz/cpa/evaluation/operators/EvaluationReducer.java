package de.tuberlin.dima.schubotz.cpa.evaluation.operators;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.EvaluationResult;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Transform result set into EvaluationResult format:
 * <p/>
 * article name | array( result1, result2, ... ) | number of results
 *
 * @param <IN> input format (Tuple)
 */
public class EvaluationReducer<IN extends Tuple> implements GroupReduceFunction<IN, EvaluationResult> {
    int maxListLength;

    public EvaluationReducer(int maxListLength) {
        this.maxListLength = maxListLength;
    }

    @Override
    public void reduce(Iterable<IN> results, Collector<EvaluationResult> out) throws Exception {
        Iterator<IN> iterator = results.iterator();
        IN record = null;

        String[] list = new String[maxListLength]; // iterator length is unknown
        int length = 0;

        while (iterator.hasNext()) {
            record = iterator.next();
            list[length] = (String) record.getField(1);
            length++;
        }
        out.collect(new EvaluationResult((String) record.getField(0), list, length));
    }
}
