package de.tuberlin.dima.schubotz.cpa.redirects;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

@RichGroupReduceFunction.Combinable
public class ReduceResults extends RichGroupReduceFunction<WikiSimRedirectResult, WikiSimRedirectResult> {

    @Override
    public void combine(Iterable<WikiSimRedirectResult> in, Collector<WikiSimRedirectResult> out) throws Exception {
        internalReduce(in, out);
    }

    @Override
    public void reduce(Iterable<WikiSimRedirectResult> in, Collector<WikiSimRedirectResult> out) throws Exception {
        internalReduce(in, out);
    }

    public void internalReduce(Iterable<WikiSimRedirectResult> in, Collector<WikiSimRedirectResult> out) throws Exception {
        Iterator<WikiSimRedirectResult> iterator = in.iterator();
        WikiSimRedirectResult reducedRecord = null;

        while (iterator.hasNext()) {
            WikiSimRedirectResult currentRecord = iterator.next();

            // init
            if (reducedRecord == null) {
                reducedRecord = currentRecord;
            } else {
                // sum
                reducedRecord.sumWith(currentRecord);
            }
        }

        out.collect(reducedRecord);
    }
}
