package org.wikipedia.citolytics.redirects.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;
import org.wikipedia.citolytics.cpa.types.list.DoubleListValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Merge duplicates created by resolved redirects
 */
public class ReduceResults implements GroupReduceFunction<WikiSimResult, WikiSimResult> {
    public static final String delimiterPattern = Pattern.quote("|");

//    @Override
//    public void combine(Iterable<WikiSimRedirectsResult2> in, Collector<WikiSimRedirectsResult2> out) throws Exception {
//        internalReduce(in, out);
//    }

    @Override
    public void reduce(Iterable<WikiSimResult> in, Collector<WikiSimResult> out) throws Exception {

        Iterator<WikiSimResult> iterator = in.iterator();

        List<WikiSimResult> records = new ArrayList<>();

        String log = "";

        // Read records
        while (iterator.hasNext()) {
            records.add(iterator.next());
        }

        // Build values
        if (records.size() > 1) {
            WikiSimResult reducedRecord = mergeResults(records);

            if (!validResult(records, reducedRecord)) {
                throw new Exception("ERROR in reduce redirected results. RecordsLength = " + records.size() + "; Count = " + reducedRecord.getCount() + "; Result = " + reducedRecord.toString() + ";\nRecords = " + records.toString() + "; \n\n" + log);
            }

            out.collect(reducedRecord);
        } else {
            out.collect(records.get(0));
        }
    }


    private WikiSimResult mergeResults(List<WikiSimResult> records) throws Exception {
        WikiSimResult res = null;

        int count = 0;
        int distance = 0;
        DoubleListValue cpa = new DoubleListValue();

        for (WikiSimResult r : records) {
            if (res == null) {
                res = new WikiSimResult(r.getPageA(), r.getPageB(), 0);
            }

            count += r.getCount();
            distance += r.getDistance();
            cpa = DoubleListValue.sum(cpa, r.getCPI());
        }

        res.setCount(count);
        res.setDistance(distance);
        res.setCPI(cpa);

        return res;
    }

    private boolean validResult(List<WikiSimResult> records, WikiSimResult result) {
        int count = 0;
        int distance = 0;

        for (WikiSimResult r : records) {
            count += r.getCount();
            distance += r.getDistance();
        }

        return count == result.getCount() && distance == result.getDistance();
    }
}
