package de.tuberlin.dima.schubotz.cpa.evaluation.better;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResultList;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class WikiSimGroupReducer implements GroupReduceFunction<Tuple3<String, String, Double>, Tuple2<String, WikiSimComparableResultList<Double>>> {
    @Override
    public void reduce(Iterable<Tuple3<String, String, Double>> in, Collector<Tuple2<String, WikiSimComparableResultList<Double>>> out) throws Exception {
        Iterator<Tuple3<String, String, Double>> iterator = in.iterator();

        Tuple3<String, String, Double> joinRecord = null;
        MinMaxPriorityQueue<WikiSimComparableResult<Double>> unsortedQueue = MinMaxPriorityQueue.maximumSize(10).create();

        while (iterator.hasNext()) {
            joinRecord = iterator.next();
            unsortedQueue.add(new WikiSimComparableResult<Double>((String) joinRecord.getField(1), (Double) joinRecord.getField(2)));
        }

        //  WikiSimComparableResultList<Double>
        out.collect(new Tuple2<>((String) joinRecord.getField(0), new WikiSimComparableResultList<Double>(unsortedQueue)));
    }
}
