package org.wikipedia.citolytics.histogram;


//import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable;
import org.apache.flink.api.common.functions.RichReduceFunction;


//@Combinable
public class HistogramReducer extends RichReduceFunction<HistogramResult> {

    public HistogramResult internalReduce(HistogramResult a, HistogramResult b) throws Exception {

        // articleCount
        a.setField((Integer) a.getField(1) + (Integer) b.getField(1), 1);

        // linkCount
        a.setField((Integer) a.getField(2) + (Integer) b.getField(2), 2);

        // linkpairCount
        a.setField((Long) a.getField(3) + (Long) b.getField(3), 3);

        return a;
    }

    @Override
    public HistogramResult reduce(HistogramResult a, HistogramResult b) throws Exception {
        return internalReduce(a, b);
    }

    public HistogramResult combine(HistogramResult a, HistogramResult b) throws Exception {
        return internalReduce(a, b);
    }
}
