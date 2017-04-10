package org.wikipedia.citolytics.seealso.operators;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;


public class SeeAlsoLinkExistsFilter implements CoGroupFunction<Tuple2<String, ArrayList<String>>, Tuple2<String, HashSet<String>>, Tuple2<String, ArrayList<String>>> {
    @Override
    public void coGroup(Iterable<Tuple2<String, ArrayList<String>>> a, Iterable<Tuple2<String, HashSet<String>>> b, Collector<Tuple2<String, ArrayList<String>>> out) throws Exception {
        Iterator<Tuple2<String, ArrayList<String>>> iteratorA = a.iterator();
        Iterator<Tuple2<String, HashSet<String>>> iteratorB = b.iterator();

        // Collect if not in HashSet
        while (iteratorA.hasNext()) {
            if (!iteratorB.hasNext()) {
                out.collect(iteratorA.next());
            } else {
                Tuple2<String, ArrayList<String>> aRecord = iteratorA.next();
                Tuple2<String, HashSet<String>> bRecord = iteratorB.next();

                ArrayList<String> testList = aRecord.getField(1);
                ArrayList<String> updatedList = new ArrayList<>();
                HashSet<String> linkList = bRecord.getField(1);

                for (String test : testList) {
                    if (!linkList.contains(test)) {
                        updatedList.add(test);
                    }
                }

                out.collect(new Tuple2<>(
                        (String) aRecord.getField(0),
                        updatedList));
            }
        }
    }
}
