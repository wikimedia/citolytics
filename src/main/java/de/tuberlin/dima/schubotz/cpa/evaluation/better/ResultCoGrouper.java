package de.tuberlin.dima.schubotz.cpa.evaluation.better;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResultList;
import de.tuberlin.dima.schubotz.cpa.evaluation.utils.EvaluationMeasures;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.shaded.com.google.common.collect.Ordering;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ResultCoGrouper implements CoGroupFunction<
        Tuple2<String, ArrayList<String>>,
        Tuple2<String, WikiSimComparableResultList<Double>>,
        Tuple7<String, ArrayList<String>, Integer, WikiSimComparableResultList<Double>, Integer, Double, Double>
        > {

    @Override
    public void coGroup(Iterable<Tuple2<String, ArrayList<String>>> a, Iterable<Tuple2<String, WikiSimComparableResultList<Double>>> b, Collector<Tuple7<String, ArrayList<String>, Integer, WikiSimComparableResultList<Double>, Integer, Double, Double>> out) throws Exception {
        Iterator<Tuple2<String, ArrayList<String>>> iteratorA = a.iterator();
        Iterator<Tuple2<String, WikiSimComparableResultList<Double>>> iteratorB = b.iterator();

        if (iteratorA.hasNext()) {
            Tuple2<String, ArrayList<String>> recordA = iteratorA.next();
            List<String> seeAlsoList = recordA.getField(1);

            List<WikiSimComparableResult<Double>> sortedList = new ArrayList<>();

            double topK = 0;
            double hrr = 0;

            if (iteratorB.hasNext()) {
                Tuple2<String, WikiSimComparableResultList<Double>> recordB = iteratorB.next();

                sortedList = Ordering.natural().greatestOf(
                        (WikiSimComparableResultList<Double>) recordB.getField(1), 10);

                List<String> resultList = new ArrayList<>();
                Iterator<WikiSimComparableResult<Double>> iterator = sortedList.listIterator();

                while (iterator.hasNext()) {
                    resultList.add(iterator.next().getName());
                }

                topK = EvaluationMeasures.getTopKScore(resultList, seeAlsoList);
                hrr = EvaluationMeasures.getHarmonicReciprocalRank(resultList, seeAlsoList);
            }

            out.collect(new Tuple7<>(
                    (String) recordA.getField(0),
                    (ArrayList<String>) recordA.getField(1),
                    ((ArrayList<String>) recordA.getField(1)).size(),
                    new WikiSimComparableResultList<Double>(sortedList),
                    sortedList.size(),
                    hrr,
                    topK
            ));
        }
    }
}
