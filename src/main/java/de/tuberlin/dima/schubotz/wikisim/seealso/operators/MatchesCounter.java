package de.tuberlin.dima.schubotz.wikisim.seealso.operators;

import de.tuberlin.dima.schubotz.wikisim.cpa.types.list.StringListValue;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.EvaluationResult;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.ListResult;
import de.tuberlin.dima.schubotz.wikisim.seealso.utils.EvaluationMeasures;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Join EvaluationResult set to EvaluationFinalResult, intersect results and count matches.
 */
public class MatchesCounter implements CoGroupFunction<EvaluationResult, ListResult, EvaluationResult> {

    int[] topKs; //= new int[]{10, 5, 1};
    int listKey;
    int matchesKey;
    int hrrKey;
    int topKKey;

    public MatchesCounter(int[] topKs, int listKey, int matchesKey, int hrrKey, int topKKey) {
        this.topKs = topKs;
        this.listKey = listKey;
        this.matchesKey = matchesKey;
        this.hrrKey = hrrKey;
        this.topKKey = topKKey;
    }

    @Override
    public void coGroup(Iterable<EvaluationResult> first, Iterable<ListResult> second, Collector<EvaluationResult> out) throws Exception {

        Iterator<EvaluationResult> iterator1 = first.iterator();
        Iterator<ListResult> iterator2 = second.iterator();

        EvaluationResult record = null;
        ListResult join = null;


        if (iterator1.hasNext()) {
            record = iterator1.next();
            StringListValue seeAlsoList = record.getField(EvaluationResult.SEEALSO_LIST_KEY);

            if (seeAlsoList.size() > 0 && iterator2.hasNext()) {
                join = iterator2.next();

                StringListValue joinList = (StringListValue) join.getField(1);
                int joinLength = joinList.size();

                // Set HRR
                record.setField(EvaluationMeasures.getHarmonicReciprocalRank(joinList.asList(), seeAlsoList.asList()), hrrKey);

                // Set TopK
                record.setField(EvaluationMeasures.getTopKScore(joinList.asList(), seeAlsoList.asList()), topKKey);

                record.setField(joinList, listKey);
                record.setField(joinLength, listKey + 1);

                int matchesCount = -1;

                for (int i = 0; i < topKs.length; i++) {
                    int topK = topKs[i];

                    if (joinLength < topK)
                        topK = joinLength;

                    // If matchesCount is already 0, avoid intersection
                    if (matchesCount != 0) {
                        matchesCount = ListUtils.intersection(seeAlsoList, joinList.subList(0, topK)).size();
                    }

                    record.setMatchesCount(matchesCount, matchesKey, i);
                }

            }

            out.collect(record);
        }
    }

}
