package org.wikipedia.citolytics.redirects.operators;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.LinkTuple;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;

import java.util.Iterator;

public class ReplaceRedirects implements CoGroupFunction<WikiSimResult, Tuple2<String, String>, WikiSimResult> {
    public int replaceField = 0;
    public int hashField = 0;
    public int pageAField = 1;
    public int pageBField = 2;
    public int redirectTargetField = 1;

    public ReplaceRedirects(int replaceField) {
        this.replaceField = replaceField;
    }

    @Override
    public void coGroup(Iterable<WikiSimResult> a, Iterable<Tuple2<String, String>> redirect, Collector<WikiSimResult> out) throws Exception {
        Iterator<WikiSimResult> iteratorA = a.iterator();
        Iterator<Tuple2<String, String>> iteratorRedirect = redirect.iterator();
        Tuple2<String, String> recordRedirect = null;

        // Redirect exists?
        if (iteratorRedirect.hasNext()) {
            recordRedirect = iteratorRedirect.next();
        }

        // Loop original records
        while (iteratorA.hasNext()) {
            WikiSimResult recordA = iteratorA.next();

            if (recordRedirect != null) {

                // replace page in original record
                recordA.setField(recordRedirect.getField(redirectTargetField), replaceField);

                // check for alphabetical order (A before B)
                int order = ((String) recordA.getField(pageAField)).compareTo(
                        (String) recordA.getField(pageBField));

                // order is wrong (negative)
                if (order > 0) {
                    // set correct order
                    String tmp = recordA.getField(pageBField);

                    recordA.setField(recordA.getField(pageAField), pageBField);
                    recordA.setField(tmp, pageAField);
                }
                // update hash
                recordA.setField(LinkTuple.getHash((String) recordA.getField(pageAField), (String) recordA.getField(pageBField)), hashField);
            }

            // Collect original record (independent of redirect)
            out.collect(recordA);
        }
    }
}
