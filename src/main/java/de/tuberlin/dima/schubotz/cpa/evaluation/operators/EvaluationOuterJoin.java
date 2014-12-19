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
public class EvaluationOuterJoin implements CoGroupFunction<EvaluationFinalResult, EvaluationResult, EvaluationFinalResult> {

    int[] firstN;
    int joinAtKey;

    public EvaluationOuterJoin(int[] firstN, int joinAtKey) {
        this.firstN = firstN;
        this.joinAtKey = joinAtKey;
    }

    @Override
    public void coGroup(Iterable<EvaluationFinalResult> first, Iterable<EvaluationResult> second, Collector<EvaluationFinalResult> out) throws Exception {

        Iterator<EvaluationFinalResult> iterator1 = first.iterator();
        Iterator<EvaluationResult> iterator2 = second.iterator();

        EvaluationFinalResult record = null;
        EvaluationResult join = null;

        if (iterator1.hasNext()) {
            record = iterator1.next();
            StringListValue recordList = record.getField(EvaluationFinalResult.SEEALSO_LIST_KEY);

            if (iterator2.hasNext()) {
                join = iterator2.next();

                StringListValue joinList = (StringListValue) join.getField(1);

                record.setField(joinList, joinAtKey);

                int matchesCount = -1;

                for (int i = 0; i < firstN.length; i++) {
                    int length = firstN[i];

                    if (joinList.size() < length)
                        length = joinList.size();

                    // If matchesCount is already 0, avoid intersection
                    if (matchesCount != 0) {
                        matchesCount = ListUtils.intersection(recordList, joinList.subList(0, length)).size();
                    }

                    record.setField(matchesCount, joinAtKey + 1 + i);
                }
            }

            out.collect(record);
        }
    }
}
