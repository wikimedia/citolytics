package de.tuberlin.dima.schubotz.cpa.evaluation.operators;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.ListResult;
import de.tuberlin.dima.schubotz.cpa.types.list.StringListValue;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Transform result set into EvaluationResult format:
 * <p/>
 * article name | array( result1, result2, ... ) | number of results
 *
 * @param <IN> input format (Tuple)
 */
public class ListBuilder<IN extends Tuple> implements GroupReduceFunction<IN, ListResult> {
    int maxListLength;

    public ListBuilder(int maxListLength) {
        this.maxListLength = maxListLength;
    }

    @Override
    public void reduce(Iterable<IN> results, Collector<ListResult> out) throws Exception {
        Iterator<IN> iterator = results.iterator();
        IN record = null;

        StringListValue list = new StringListValue();
//        String[] list = new String[maxListLength]; // iterator length is unknown
        int length = 0;

        while (iterator.hasNext()) {
            record = iterator.next();
//            list[length] = (String) record.getField(1);
            list.add(new StringValue((String) record.getField(1)));
            length++;
        }
        out.collect(new ListResult((String) record.getField(0), list, length));
    }
}
