package org.wikipedia.citolytics.seealso.operators;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.Recommendation;

import java.util.HashSet;
import java.util.Iterator;


public class BetterLinkExistsFilter implements CoGroupFunction<Recommendation, Tuple2<String, HashSet<String>>, Recommendation> {
    @Override
    public void coGroup(Iterable<Recommendation> a, Iterable<Tuple2<String, HashSet<String>>> b, Collector<Recommendation> out) throws Exception {
        Iterator<Recommendation> iteratorA = a.iterator();
        Iterator<Tuple2<String, HashSet<String>>> iteratorB = b.iterator();

        // Collect if not in HashSet
        while (iteratorA.hasNext()) {
            if (!iteratorB.hasNext()) {
                out.collect(iteratorA.next());
            } else {
                Recommendation aRecord = iteratorA.next();
                Tuple2<String, HashSet<String>> bRecord = iteratorB.next();

                if (!((HashSet) bRecord.getField(1)).contains(aRecord.getField(1))) {
                    out.collect(aRecord);
                }
            }
        }
    }
}
