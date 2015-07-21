package de.tuberlin.dima.schubotz.wikisim.seealso.better;

import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.wikisim.seealso.types.WikiSimComparableResultList;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Iterator;

public class WikiSimGroupReducer implements GroupReduceFunction<Tuple3<String, String, Double>, Tuple2<String, WikiSimComparableResultList<Double>>> {
    public int maxQueueSize = 20;

    public WikiSimGroupReducer() {
    }

    public WikiSimGroupReducer(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    public void reduce(Iterable<Tuple3<String, String, Double>> in, Collector<Tuple2<String, WikiSimComparableResultList<Double>>> out) throws Exception {
        Iterator<Tuple3<String, String, Double>> iterator = in.iterator();

        Tuple3<String, String, Double> joinRecord = null;

        // Default: MinQueue = Smallest elements a kept in queue
        // -> Change: MaxQueue = Keep greatest elements
        MinMaxPriorityQueue<WikiSimComparableResult<Double>> unsortedQueue = MinMaxPriorityQueue
                .orderedBy(new Comparator<WikiSimComparableResult<Double>>() {
                    @Override
                    public int compare(WikiSimComparableResult<Double> o1, WikiSimComparableResult<Double> o2) {
                        return -1 * o1.compareTo(o2);
                    }
                })
                .maximumSize(maxQueueSize)
                .create();

        while (iterator.hasNext()) {
            joinRecord = iterator.next();
            unsortedQueue.add(new WikiSimComparableResult<Double>((String) joinRecord.getField(1), (Double) joinRecord.getField(2)));
        }

        //  WikiSimComparableResultList<Double>
        out.collect(new Tuple2<>((String) joinRecord.getField(0), new WikiSimComparableResultList<Double>(unsortedQueue)));
    }
}
