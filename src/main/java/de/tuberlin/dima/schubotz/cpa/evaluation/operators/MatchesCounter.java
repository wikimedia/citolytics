package de.tuberlin.dima.schubotz.cpa.evaluation.operators;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.EvaluationResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.ListResult;
import de.tuberlin.dima.schubotz.cpa.types.list.StringListValue;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Join EvaluationResult set to EvaluationFinalResult, intersect results and count matches.
 */
public class MatchesCounter implements CoGroupFunction<EvaluationResult, ListResult, EvaluationResult> {

    int[] firstN;
    int listKey;
    int matchesKey;

    public MatchesCounter(int[] firstN, int listKey, int matchesKey) {
        this.firstN = firstN;
        this.listKey = listKey;
        this.matchesKey = matchesKey;
    }

    @Override
    public void coGroup(Iterable<EvaluationResult> first, Iterable<ListResult> second, Collector<EvaluationResult> out) throws Exception {

        Iterator<EvaluationResult> iterator1 = first.iterator();
        Iterator<ListResult> iterator2 = second.iterator();

        EvaluationResult record = null;
        ListResult join = null;


        if (iterator1.hasNext()) {
            record = iterator1.next();
            StringListValue recordList = record.getField(EvaluationResult.SEEALSO_LIST_KEY);
//            String[] recordList = record.getField(EvaluationFinalResult.SEEALSO_LIST_KEY);

            if (iterator2.hasNext()) {
                join = iterator2.next();

                StringListValue joinList = (StringListValue) join.getField(1);
                int joinLength = joinList.size();
//                String[] joinList = (String[]) join.getField(1);
//                int joinLength = join.getField(2);

                // Set MRR
                record.setField(getMeanReciprocalRank(joinList, recordList), listKey + 2);

                record.setField(joinList, listKey);
                record.setField(joinLength, listKey + 1);

                int matchesCount = -1;

                for (int i = 0; i < firstN.length; i++) {
                    int length = firstN[i];

                    if (joinLength < length)
                        length = joinLength;

                    // If matchesCount is already 0, avoid intersection
                    if (matchesCount != 0) {
//                        String[] subList = Arrays.copyOfRange(joinList, 0, length);
                        matchesCount = ListUtils.intersection(recordList, joinList.subList(0, length)).size();
//                        matchesCount = getIntersectionCount(subList, recordList);
                    }

                    record.setMatchesCount(matchesCount, matchesKey, i);
//                    record.setField(matchesCount, matchesKey + i);
                }
            }

            out.collect(record);
        }
    }

    // Optimize runtime?
    public static int getIntersectionCount(String[] a, String[] b) {

        int matches = 0;
        for (String anA : a) {
            for (String anB : b) {
                if (anA.equals(anB)) {
                    matches++;
                }
            }
        }
//        System.out.println(Arrays.toString(a));
//        System.out.println("---- " + Arrays.toString(b));
//        System.out.println("---- " + matches);

        return matches;
    }

    public static double getMeanReciprocalRank(StringListValue sortedResults, StringListValue correctResponseList) {
        int rank = 0;
        for (StringValue correct : correctResponseList) {
            rank += getMeanReciprocalRank(sortedResults, correct);
        }
        return rank;
    }

    public static double getMeanReciprocalRank(StringListValue sortedResults, StringValue correctResponse) {
        int rank = sortedResults.indexOf(correctResponse);

        if (rank == -1) {
            return 0;
        } else {
            return 1 / (rank + 1);
        }
    }
}
