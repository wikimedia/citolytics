package de.tuberlin.dima.schubotz.cpa.evaluation.operators;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.EvaluationFinalResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.EvaluationResult;
import de.tuberlin.dima.schubotz.cpa.types.StringListValue;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Created by malteschwarzer on 11.12.14.
 */
public class OuterJoin implements CoGroupFunction<EvaluationFinalResult, EvaluationResult, EvaluationFinalResult> {

    int joinAtKey;

    public OuterJoin(int joinAtKey) {
        this.joinAtKey = joinAtKey;
    }

    @Override
    public void coGroup(Iterable<EvaluationFinalResult> first, Iterable<EvaluationResult> second, Collector<EvaluationFinalResult> out) throws Exception {

        Iterator<EvaluationFinalResult> iterator1 = first.iterator();
        Iterator<EvaluationResult> iterator2 = second.iterator();

        EvaluationFinalResult record = null;
        EvaluationResult join = null;

        StringListValue emptyList = StringListValue.valueOf(new String[]{});

        if (iterator1.hasNext()) {
            record = iterator1.next();
            StringListValue recordList = record.getField(1);

            if (iterator2.hasNext()) {
                join = iterator2.next();

                StringListValue joinList = (StringListValue) join.getField(1);

                record.setField(joinList, joinAtKey);
                record.setField(ListUtils.intersection(recordList, joinList).size(), joinAtKey + 1);
            }

            out.collect(record);
        }
    }
}
