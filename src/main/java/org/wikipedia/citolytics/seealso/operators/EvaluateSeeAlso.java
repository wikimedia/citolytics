package org.wikipedia.citolytics.seealso.operators;

import com.google.common.collect.Ordering;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.seealso.types.SeeAlsoEvaluationResult;
import org.wikipedia.citolytics.seealso.types.SeeAlsoLinks;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;
import org.wikipedia.citolytics.seealso.utils.EvaluationMeasures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class EvaluateSeeAlso implements CoGroupFunction<
        SeeAlsoLinks,
        RecommendationSet,
        SeeAlsoEvaluationResult
        > {

    private int topK = 10;

    public EvaluateSeeAlso(int topK) {
        this.topK = topK;
    }


    @Override
    public void coGroup(Iterable<SeeAlsoLinks> a, Iterable<RecommendationSet> b, Collector<SeeAlsoEvaluationResult> out) throws Exception {
        Iterator<SeeAlsoLinks> iteratorA = a.iterator();
        Iterator<RecommendationSet> iteratorB = b.iterator();

        if (iteratorA.hasNext()) {
            SeeAlsoLinks seeAlsoLinks = iteratorA.next();
            Set<String> seeAlsoList = seeAlsoLinks.getLinks();

            List<WikiSimComparableResult<Double>> sortedList = new ArrayList<>();

            double topKScore = 0;
            double mrr = 0;
            double map = 0;

            int[] matches = new int[]{0, 0, 0};

            if (iteratorB.hasNext()) {
                RecommendationSet recordB = iteratorB.next();

                sortedList = Ordering.natural().greatestOf(recordB.getResults(), topK);

                List<String> resultList = getResultNamesAsList(sortedList);

                topKScore = EvaluationMeasures.getTopKScore(resultList, new ArrayList<>(seeAlsoList));
                mrr = EvaluationMeasures.getMeanReciprocalRank(resultList, new ArrayList<>(seeAlsoList));;
                map = EvaluationMeasures.getMeanAveragePrecision(resultList, seeAlsoList);

                matches = EvaluationMeasures.getMatchesCount(resultList, new ArrayList<>(seeAlsoList));
            }

            out.collect(new SeeAlsoEvaluationResult(
                    seeAlsoLinks.getArticle(),
                    seeAlsoLinks.getLinks(),
                    seeAlsoLinks.getLinks().size(),
                    new WikiSimComparableResultList<>(sortedList),
                    sortedList.size(),
                    mrr,
                    topKScore,
                    map,
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
