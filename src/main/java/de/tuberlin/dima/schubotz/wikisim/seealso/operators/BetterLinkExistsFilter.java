package de.tuberlin.dima.schubotz.wikisim.seealso.operators;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;


public class BetterLinkExistsFilter implements CoGroupFunction<Tuple3<String, String, Double>, Tuple2<String, HashSet<String>>, Tuple3<String, String, Double>> {
    @Override
    public void coGroup(Iterable<Tuple3<String, String, Double>> a, Iterable<Tuple2<String, HashSet<String>>> b, Collector<Tuple3<String, String, Double>> out) throws Exception {
        Iterator<Tuple3<String, String, Double>> iteratorA = a.iterator();
        Iterator<Tuple2<String, HashSet<String>>> iteratorB = b.iterator();

        // Collect if not in HashSet
        while (iteratorA.hasNext()) {
            if (!iteratorB.hasNext()) {
                out.collect(iteratorA.next());
            } else {
                Tuple3<String, String, Double> aRecord = iteratorA.next();
                Tuple2<String, HashSet<String>> bRecord = iteratorB.next();

                if (!((HashSet) bRecord.getField(1)).contains(aRecord.getField(1))) {
                    out.collect(aRecord);
                }
            }
        }
    }
}
