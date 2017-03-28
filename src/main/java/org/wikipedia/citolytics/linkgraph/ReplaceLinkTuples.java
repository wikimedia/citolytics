package org.wikipedia.citolytics.linkgraph;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;

import java.util.Iterator;

public class ReplaceLinkTuples implements CoGroupFunction<Tuple2<String, String>, RedirectMapping, Tuple2<String, String>> {
    public int inField = 0;

    public ReplaceLinkTuples(int inField) {
        this.inField = inField;
    }

    @Override
    public void coGroup(Iterable<Tuple2<String, String>> in, Iterable<RedirectMapping> redirect, Collector<Tuple2<String, String>> out) throws Exception {
        Iterator<Tuple2<String, String>> iteratorIn = in.iterator();
        Iterator<RedirectMapping> iteratorRedirect = redirect.iterator();

        while (iteratorIn.hasNext()) {
            Tuple2<String, String> recordIn = iteratorIn.next();

            out.collect(recordIn);

            while (iteratorRedirect.hasNext()) {
                RedirectMapping recordRedirect = iteratorRedirect.next();

//                System.out.println(recordRedirect);
//                System.out.println(recordIn);

                // replace
                recordIn.setField(recordRedirect.getSource(), inField);
                out.collect(recordIn);

//                System.out.println(" + " + recordIn);
            }

        }
    }
}
