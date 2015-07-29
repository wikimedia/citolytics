package de.tuberlin.dima.schubotz.wikisim.redirects;

import de.tuberlin.dima.schubotz.wikisim.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.wikisim.cpa.types.list.DoubleListValue;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
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
        internalReduce(in, out);
    }

    public void internalReduce(Iterable<WikiSimResult> in, Collector<WikiSimResult> out) throws Exception {
        Iterator<WikiSimResult> iterator = in.iterator();
        WikiSimResult reducedRecord = null;

        // Build values
        while (iterator.hasNext()) {

            WikiSimResult currentRecord = iterator.next();
            // init
            if (reducedRecord == null) {
                reducedRecord = currentRecord;
            } else {
                // Count
                reducedRecord.setCount(reducedRecord.getCount() + currentRecord.getCount());

                // Distance
                reducedRecord.setDistance(reducedRecord.getDistance() + currentRecord.getDistance());

                // CPA
                reducedRecord.setCPA(DoubleListValue.sum(reducedRecord.getCPA(), currentRecord.getCPA()));
            }
        }

        out.collect(reducedRecord);
    }
}
