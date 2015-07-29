package de.tuberlin.dima.schubotz.wikisim.seealso.operators;

import de.tuberlin.dima.schubotz.wikisim.cpa.types.list.StringListValue;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.EvaluationResult;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResult;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Join EvaluationResult set to EvaluationFinalResult, intersect results and count matches.
 * <p/>
 * WikiSim results not as list -> single results + not sorted
 * *
 * ResultRecords are not
 */

@Deprecated
public class MatchesCounterPlainResults<SORT extends Comparable> implements CoGroupFunction<EvaluationResult, Tuple3<String, String, SORT>, EvaluationResult> {

    int[] topKs; //= new int[]{10, 5, 1};
    int listKey;
    int matchesKey;
    int hrrKey;
    int topKKey;

    public MatchesCounterPlainResults(int[] topKs, int listKey, int matchesKey, int hrrKey, int topKKey) {
        this.topKs = topKs;
        this.listKey = listKey;
        this.matchesKey = matchesKey;
        this.hrrKey = hrrKey;
        this.topKKey = topKKey;
    }

    @Override
    public void coGroup(Iterable<EvaluationResult> first, Iterable<Tuple3<String, String, SORT>> second, Collector<EvaluationResult> out) throws Exception {

        Iterator<EvaluationResult> iterator1 = first.iterator();
        Iterator<Tuple3<String, String, SORT>> iterator2 = second.iterator();

        EvaluationResult record = null;
        Tuple3<String, String, SORT> joinRecord = null;


        if (iterator1.hasNext()) {
            record = iterator1.next();
            StringListValue seeAlsoList = record.getField(EvaluationResult.SEEALSO_LIST_KEY);

            // Build List
            List<WikiSimComparableResult<SORT>> unsortedList = new ArrayList<>();

            while (iterator2.hasNext()) {
                joinRecord = iterator2.next();
                unsortedList.add(new WikiSimComparableResult<SORT>((String) joinRecord.getField(1), (SORT) joinRecord.getField(2)));
//                unsortedList.addAll((List<WikiSimComparableResult<SORT>>) joinRecord.getField(1));
            }
            // Sort List

            // Count matches


//
//            if (seeAlsoList.size() > 0 && iterator2.hasNext()) {
//                join = iterator2.next();
//
//                StringListValue joinList = (StringListValue) join.getField(1);
//                int joinLength = joinList.size();
//
//                // Set HRR
//                record.setField(getHarmonicReciprocalRank(joinList, seeAlsoList), hrrKey);
//
//                // Set TopK
//                record.setField(getTopKScore(joinList, seeAlsoList), topKKey);
//
//                record.setField(joinList, listKey);
//                record.setField(joinLength, listKey + 1);
//
//                int matchesCount = -1;
//
//                for (int i = 0; i < topKs.length; i++) {
//                    int topK = topKs[i];
//
//                    if (joinLength < topK)
//                        topK = joinLength;
//
//                    // If matchesCount is already 0, avoid intersection
//                    if (matchesCount != 0) {
//                        matchesCount = ListUtils.intersection(seeAlsoList, joinList.subList(0, topK)).size();
//                    }
//
//                    record.setMatchesCount(matchesCount, matchesKey, i);
//                }
//
//            }

            out.collect(record);
        }
    }

    public static double getTopKScore(StringListValue recommendedResults, StringListValue correctResponseList) {
        // topK = correct recommended results / total number of correct results
        if (recommendedResults.size() == 0)
            return 0;

        int subListLength = correctResponseList.size();

        if (recommendedResults.size() < correctResponseList.size()) {
            subListLength = recommendedResults.size();
        }

        // cast to double to receive double result
        return ((double) ListUtils.intersection(correctResponseList, recommendedResults.subList(0, subListLength)).size())
                / ((double) correctResponseList.size());
    }

    public static double getHarmonicReciprocalRank(StringListValue recommendedResults, StringListValue correctResponseList) {
        double rank = 0;

        if (correctResponseList.size() == 0)
            return 0;

        for (StringValue response : recommendedResults) {
            rank += getHarmonicReciprocalRank(correctResponseList, response);
        }

        return rank / getHarmonicNumber(correctResponseList.size());
    }

    public static double getHarmonicNumber(int n) {
        double sum = 0.0;
        for (int i = 1; i <= n; i++) {
            //sum += 1.0 / i;
            sum += (1.0 / i);
        }
        return sum;
    }

    public static double getHarmonicReciprocalRank(StringListValue correctResults, StringValue recomendedResult) {
        int rank = correctResults.indexOf(recomendedResult) + 1;
        double hrr = 0;

        if (rank > 0) {
            hrr = 1.0 / rank;
        }
        return hrr;
    }

    public static double getMeanReciprocalRank(StringListValue sortedResults, StringListValue correctResponseList) {
        double rank = 0;

        if (correctResponseList.size() == 0)
            return 0;

        for (StringValue correct : correctResponseList) {
            rank += getMeanReciprocalRank(sortedResults, correct);
        }

        return rank / correctResponseList.size();
    }

    public static double getMeanReciprocalRank(StringListValue sortedResults, StringValue correctResponse) {
        int rank = sortedResults.indexOf(correctResponse) + 1;
        double mrr = 0;

        if (rank > 0) {
            mrr = 1.0 / rank;
        }
        return mrr;
    }
}
