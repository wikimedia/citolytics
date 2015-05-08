package de.tuberlin.dima.schubotz.cpa.redirects;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class ReplaceRedirects implements CoGroupFunction<WikiSimRedirectResult, Tuple2<String, String>, WikiSimRedirectResult> {
    public int replaceField = 0;
    public int pageAField = 1;
    public int pageBField = 2;
    public int redirectTargetField = 1;

    public ReplaceRedirects(int replaceField) {
        this.replaceField = replaceField;
    }

    @Override
    public void coGroup(Iterable<WikiSimRedirectResult> a, Iterable<Tuple2<String, String>> redirect, Collector<WikiSimRedirectResult> out) throws Exception {
        Iterator<WikiSimRedirectResult> iteratorA = a.iterator();
        Iterator<Tuple2<String, String>> iteratorRedirect = redirect.iterator();

        while (iteratorA.hasNext()) {
            WikiSimRedirectResult recordA = iteratorA.next();

            if (iteratorRedirect.hasNext()) {
                Tuple2<String, String> recordRedirect = iteratorRedirect.next();

                // replace
                recordA.setField(recordRedirect.getField(redirectTargetField), replaceField);

                // check for alphabetic order
                int order = ((String) recordA.getField(pageBField)).compareTo((String) recordA.getField(pageAField));
                if (order < 0) {
                    // correct order
                    String tmp = recordA.getField(pageBField);

                    recordA.setField(recordA.getField(pageAField), pageBField);
                    recordA.setField(tmp, pageAField);
                }
            }
            out.collect(recordA);
        }
    }
}
