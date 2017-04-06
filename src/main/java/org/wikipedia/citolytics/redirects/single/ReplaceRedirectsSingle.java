package org.wikipedia.citolytics.redirects.single;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.LinkPair;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;

import java.util.Iterator;

public class ReplaceRedirectsSingle implements CoGroupFunction<WikiSimRedirectsResult, RedirectMapping, WikiSimRedirectsResult> {
    public int replaceField = 0;
    public int hashField = 0;
    public int pageAField = 1;
    public int pageBField = 2;
    public int redirectTargetField = 1;

    public ReplaceRedirectsSingle(int replaceField) {
        this.replaceField = replaceField;
    }

    @Override
    public void coGroup(Iterable<WikiSimRedirectsResult> a, Iterable<RedirectMapping> redirect, Collector<WikiSimRedirectsResult> out) throws Exception {
        Iterator<WikiSimRedirectsResult> iteratorA = a.iterator();
        Iterator<RedirectMapping> iteratorRedirect = redirect.iterator();
        RedirectMapping recordRedirect = null;

        // Redirect exists?
        if (iteratorRedirect.hasNext()) {
            recordRedirect = iteratorRedirect.next();
        }

        // Loop original records
        while (iteratorA.hasNext()) {
            WikiSimRedirectsResult recordA = iteratorA.next();

            if (recordRedirect != null) {

                // replace page in original record
                recordA.setField(recordRedirect.getTarget(), replaceField);

                // check for alphabetic order
                int order = ((String) recordA.getField(pageBField)).compareTo((String) recordA.getField(pageAField));
                if (order < 0) {
                    // correct order
                    String tmp = recordA.getField(pageBField);

                    recordA.setField(recordA.getField(pageAField), pageBField);
                    recordA.setField(tmp, pageAField);
                }
                // update hash
                recordA.setField(LinkPair.getHash((String) recordA.getField(pageAField), (String) recordA.getField(pageBField)), hashField);
            }

            // Collect original record (independent of redirect)
            out.collect(recordA);
        }
    }
}
