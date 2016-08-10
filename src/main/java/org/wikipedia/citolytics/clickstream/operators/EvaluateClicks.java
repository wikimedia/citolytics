package org.wikipedia.citolytics.clickstream.operators;

import com.google.common.collect.Ordering;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Calculates CTR, total clicks, impressions for each article in result set.
 */
public class EvaluateClicks implements CoGroupFunction<Tuple2<String, WikiSimComparableResultList<Double>>, Tuple3<String, Integer, HashMap<String, Integer>>, ClickStreamResult> {

    private static int[] k = new int[]{10, 5, 1};
    private static int topK = 10;

    public EvaluateClicks() {
    }

    public EvaluateClicks(int topK) {
        this.topK = topK;
    }


    @Override
    public void coGroup(Iterable<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimRecords, Iterable<Tuple3<String, Integer, HashMap<String, Integer>>> clickStreamRecords, Collector<ClickStreamResult> out) throws Exception {
        Iterator<Tuple2<String, WikiSimComparableResultList<Double>>> wikiSimIterator = wikiSimRecords.iterator();
        Iterator<Tuple3<String, Integer, HashMap<String, Integer>>> clickStreamIterator = clickStreamRecords.iterator();

        // Proceed only when both records exist
        if (!wikiSimIterator.hasNext() || !clickStreamIterator.hasNext()) {
            return;
        }


        // Fetch from iterators
        Tuple2<String, WikiSimComparableResultList<Double>> wikiSimRecord = wikiSimIterator.next();
        Tuple3<String, Integer, HashMap<String, Integer>> clickStreamRecord = clickStreamIterator.next();

        // Sort and get top-k results
        List<WikiSimComparableResult<Double>> retrievedDocuments = Ordering.natural().greatestOf(wikiSimRecord.f1, topK);


        HashMap<String, Integer> clickStream = clickStreamRecord.f2;
        int impressions = clickStreamRecord.f1;


        // Initialize output vars
        int[] clicksK = new int[]{0, 0, 0};
        int totalClicks = 0;
        ArrayList<Tuple3<String, Double, Integer>> results = new ArrayList<>();

        // Clicks on retrieved docs
        int rank = 1;
        for (WikiSimComparableResult doc : retrievedDocuments) {
            int clicks = clickStream.containsKey(doc.getName()) ? clickStream.get(doc.getName()) : 0;

            results.add(new Tuple3<>(
                    doc.getName(),
                    (Double) doc.getSortField1(),
                    clicks
            ));

            clicksK = calculateTotalClicks(clicksK, rank, clicks);
            rank++;
        }

        // Count all out clicks
        for (Integer c : clickStream.values())
            totalClicks += c;


        ClickStreamResult res = new ClickStreamResult();

        res.f0 = wikiSimRecord.f0;
        res.f1 = results;
        res.f2 = results.size();
        res.f3 = impressions;
        res.f4 = totalClicks;
        res.f5 = clicksK[0];
        res.f6 = clicksK[1];
        res.f7 = clicksK[2];

        out.collect(
                res
        );
    }

    private int[] calculateTotalClicks(int[] clicksK, int rank, int clicks) {
        // loop k's
        for (int i = 0; i < k.length; i++) {
            if (rank <= k[i]) {
                clicksK[i] += clicks;
            }
        }
        return clicksK;
    }
}
