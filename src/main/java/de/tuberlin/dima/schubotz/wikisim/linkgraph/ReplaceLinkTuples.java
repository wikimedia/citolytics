package de.tuberlin.dima.schubotz.wikisim.linkgraph;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class ReplaceLinkTuples implements CoGroupFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> {
    public int inField = 0;

    public ReplaceLinkTuples(int inField) {
        this.inField = inField;
    }

    @Override
    public void coGroup(Iterable<Tuple2<String, String>> in, Iterable<Tuple2<String, String>> redirect, Collector<Tuple2<String, String>> out) throws Exception {
        Iterator<Tuple2<String, String>> iteratorIn = in.iterator();
        Iterator<Tuple2<String, String>> iteratorRedirect = redirect.iterator();

        while (iteratorIn.hasNext()) {
            Tuple2<String, String> recordIn = iteratorIn.next();

            out.collect(recordIn);

            while (iteratorRedirect.hasNext()) {
                Tuple2<String, String> recordRedirect = iteratorRedirect.next();

//                System.out.println(recordRedirect);
//                System.out.println(recordIn);

                // replace
                recordIn.setField(recordRedirect.getField(0), inField);
                out.collect(recordIn);

//                System.out.println(" + " + recordIn);
            }

        }
    }
}
