package org.wikipedia.citolytics.seealso.better;

import com.google.common.collect.Ordering;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.seealso.types.SeeAlsoEvaluationResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;
import org.wikipedia.citolytics.seealso.utils.EvaluationMeasures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EvaluateSeeAlso implements CoGroupFunction<
        Tuple2<String, ArrayList<String>>,
        RecommendationSet,
        SeeAlsoEvaluationResult
        > {

    private int topK = 10;
    private boolean enableMRR = false; // If true, MRR is used instead of MAP

    public EvaluateSeeAlso(int topK) {
        this.topK = topK;
    }

    public EvaluateSeeAlso(int topK, boolean enableMRR) {
        this.topK = topK;
        this.enableMRR = enableMRR;
    }

    @Override
    public void coGroup(Iterable<Tuple2<String, ArrayList<String>>> a, Iterable<RecommendationSet> b, Collector<SeeAlsoEvaluationResult> out) throws Exception {
        Iterator<Tuple2<String, ArrayList<String>>> iteratorA = a.iterator();
        Iterator<RecommendationSet> iteratorB = b.iterator();

        if (iteratorA.hasNext()) {
            Tuple2<String, ArrayList<String>> recordA = iteratorA.next();
            List<String> seeAlsoList = recordA.getField(1);

            List<WikiSimComparableResult<Double>> sortedList = new ArrayList<>();

            double topKScore = 0;
            double hrr = 0;
            double performance = 0;

            int[] matches = new int[]{0, 0, 0};

            if (iteratorB.hasNext()) {
                RecommendationSet recordB = iteratorB.next();

                sortedList = Ordering.natural().greatestOf(recordB.getResults(), topK);

                List<String> resultList = getResultNamesAsList(sortedList);

                topKScore = EvaluationMeasures.getTopKScore(resultList, seeAlsoList);
                hrr = EvaluationMeasures.getHarmonicReciprocalRank(resultList, seeAlsoList);

                if (enableMRR) {
                    performance = EvaluationMeasures.getMeanReciprocalRank(resultList, seeAlsoList);
                } else {
                    performance = EvaluationMeasures.getMeanAveragePrecision(resultList, seeAlsoList);
                }

                matches = EvaluationMeasures.getMatchesCount(resultList, seeAlsoList);
            }

            out.collect(new SeeAlsoEvaluationResult(
                    (String) recordA.getField(0),
                    (ArrayList<String>) recordA.getField(1),
                    ((ArrayList<String>) recordA.getField(1)).size(),
                    new WikiSimComparableResultList<>(sortedList),
                    sortedList.size(),
                    hrr,
                    topKScore,
                    performance,
                    matches[0],
                    matches[1],
                    matches[2]
            ));
        }
    }

    public static List<String> getResultNamesAsList(List<WikiSimComparableResult<Double>> sortedList) {
        List<String> resultList = new ArrayList<>();

        for (WikiSimComparableResult<Double> aSortedList : sortedList) {
            resultList.add(aSortedList.getName());
        }
        return resultList;
    }
}
