package org.wikipedia.citolytics.clickstream.operators;

//import com.google.common.collect.Ordering;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.shaded.com.google.common.collect.Ordering;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.clickstream.types.ClickStreamRecommendationResult;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;
import org.wikipedia.citolytics.cpa.types.WikiSimTopResults;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Calculates CTR, total clicks, impressions for each article in result set.
 */
public class EvaluateClicks implements CoGroupFunction<WikiSimTopResults, ClickStreamTuple, ClickStreamResult> {
    private final static boolean IGNORE_MISSING_CLICK_STREAM = false;
    private int[] k = new int[]{10, 5, 1};
    private int topK = 10;

    public EvaluateClicks() {
    }

    public EvaluateClicks(int topK) {
        this.topK = topK;
    }

    @Override
    public void coGroup(Iterable<WikiSimTopResults> wikiSimRecords, Iterable<ClickStreamTuple> clickStreamRecords, Collector<ClickStreamResult> out) throws Exception {
        Iterator<WikiSimTopResults> wikiSimIterator = wikiSimRecords.iterator();
        Iterator<ClickStreamTuple> clickStreamIterator = clickStreamRecords.iterator();

        // Proceed only recommendation records exist
        if (!wikiSimIterator.hasNext()) {
            return;
        }

        // Fetch from iterators
        WikiSimTopResults wikiSimRecord = wikiSimIterator.next();

        // It's ok if click stream does not exist
        HashMap<String, Integer> clickStream;
        int impressions = 0;

        if(!IGNORE_MISSING_CLICK_STREAM && !clickStreamIterator.hasNext()) {
            return;
        }

        ClickStreamTuple clickStreamRecord = clickStreamIterator.next();
        clickStream = clickStreamRecord.getOutClicks();
        impressions = clickStreamRecord.getImpressions();

        System.out.println("CS found - " + wikiSimRecord.getSourceTitle() + ": " + clickStreamRecord);

        // Sort and get top-k results
        List<WikiSimComparableResult<Double>> retrievedDocuments = Ordering.natural().greatestOf(wikiSimRecord.getResults(), topK);

        // Initialize output vars
        int[] clicksK = new int[]{0, 0, 0};
        int totalClicks = 0;
        ArrayList<ClickStreamRecommendationResult> results = new ArrayList<>();

        // Clicks on retrieved docs
        int rank = 1;
        for (WikiSimComparableResult doc : retrievedDocuments) {
            int clicks = clickStream.containsKey(doc.getName()) ? clickStream.get(doc.getName()) : 0;

            results.add(new ClickStreamRecommendationResult(
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

        out.collect(
            new ClickStreamResult(
                    wikiSimRecord.getSourceTitle(),
                    results,
                    results.size(),
                    impressions,
                    totalClicks,
                    clicksK[0],
                    clicksK[1],
                    clicksK[2]
            )
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
