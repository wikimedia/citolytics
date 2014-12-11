package de.tuberlin.dima.schubotz.cpa.evaluation.operators;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.EvaluationResult;
import de.tuberlin.dima.schubotz.cpa.types.StringListValue;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class EvaluationReducer<IN extends Tuple> implements GroupReduceFunction<IN, EvaluationResult> {
    @Override
    public void reduce(Iterable<IN> results, Collector<EvaluationResult> out) throws Exception {
        Iterator<IN> iterator = results.iterator();
        IN record = null;
        StringListValue list = new StringListValue();

        while (iterator.hasNext()) {
            record = iterator.next();
            list.add(new StringValue((String) record.getField(1)));
        }
        out.collect(new EvaluationResult((String) record.getField(0), list));
    }
}
