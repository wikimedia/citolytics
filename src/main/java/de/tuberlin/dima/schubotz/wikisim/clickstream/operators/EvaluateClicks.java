package de.tuberlin.dima.schubotz.wikisim.clickstream.operators;

import de.tuberlin.dima.schubotz.wikisim.clickstream.types.ClickStreamResult;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResultList;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Calculates CTR, total clicks, impressions for each article in result set.
 */
public class EvaluateClicks implements CoGroupFunction<Tuple2<String, WikiSimComparableResultList<Double>>, Tuple3<String, Integer, HashMap<String, Integer>>, ClickStreamResult> {
    private int outClicks = 0;
    private int[] k = new int[]{10, 5, 1};
    private int[] totalClicks = new int[]{0, 0, 0};

    @Override
    public void coGroup(Iterable<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimRecords, Iterable<Tuple3<String, Integer, HashMap<String, Integer>>> clickStreamRecords, Collector<ClickStreamResult> out) throws Exception {
        Iterator<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimIterator = wikiSimRecords.iterator();
        Iterator<Tuple3<String, Integer, HashMap<String, Integer>>> clickStreamIterator = clickStreamRecords.iterator();

        // Proceed only when both records exist
        if (!wikiSimIterator.hasNext() || !clickStreamIterator.hasNext()) {
            return;
        }

        Tuple2<String, WikiSimComparableResultList<Double>> wikiSimRecord = wikiSimIterator.next();

        WikiSimComparableResultList<Double> retrievedDocuments = wikiSimRecord.f1;

        ArrayList<Tuple3<String, Double, Integer>> results = new ArrayList<>();

        Tuple3<String, Integer, HashMap<String, Integer>> clickStreamRecord = clickStreamIterator.next();

        HashMap<String, Integer> clickStream = clickStreamRecord.f2;
        int impressions = clickStreamRecord.f1;

        int rank = 1;

        // Clicks on retrieved docs
        for (WikiSimComparableResult doc : retrievedDocuments) {
            int clicks = clickStream.containsKey(doc.getName()) ? clickStream.get(doc.getName()) : 0;

            results.add(new Tuple3<>(
                    doc.getName(),
                    (Double) doc.getSortField1(),
                    clicks
            ));

            calculateTotalClicks(rank, clicks);
            rank++;
        }

        // All out clicks
        for (Integer c : clickStream.values())
            outClicks += c;


        ClickStreamResult res = new ClickStreamResult();

        res.f0 = wikiSimRecord.f0;
        res.f1 = results;
        res.f2 = results.size();
        res.f3 = impressions;
        res.f4 = outClicks;
        res.f5 = totalClicks[0];
        res.f6 = totalClicks[1];
        res.f7 = totalClicks[2];

        out.collect(
                res
        );
    }

    private void calculateTotalClicks(int rank, int clicks) {
        // loop k's
        for (int i = 0; i < k.length; i++) {
            if (rank <= k[i]) {
                totalClicks[i] += clicks;
            }
        }
    }
}
