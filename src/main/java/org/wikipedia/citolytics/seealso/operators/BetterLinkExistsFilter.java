package org.wikipedia.citolytics.seealso.operators;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.WikiSimRecommendation;

import java.util.HashSet;
import java.util.Iterator;


public class BetterLinkExistsFilter implements CoGroupFunction<WikiSimRecommendation, Tuple2<String, HashSet<String>>, WikiSimRecommendation> {
    @Override
    public void coGroup(Iterable<WikiSimRecommendation> a, Iterable<Tuple2<String, HashSet<String>>> b, Collector<WikiSimRecommendation> out) throws Exception {
        Iterator<WikiSimRecommendation> iteratorA = a.iterator();
        Iterator<Tuple2<String, HashSet<String>>> iteratorB = b.iterator();

        // Collect if not in HashSet
        while (iteratorA.hasNext()) {
            if (!iteratorB.hasNext()) {
                out.collect(iteratorA.next());
            } else {
                WikiSimRecommendation aRecord = iteratorA.next();
                Tuple2<String, HashSet<String>> bRecord = iteratorB.next();

                if (!((HashSet) bRecord.getField(1)).contains(aRecord.getField(1))) {
                    out.collect(aRecord);
                }
            }
        }
    }
}
