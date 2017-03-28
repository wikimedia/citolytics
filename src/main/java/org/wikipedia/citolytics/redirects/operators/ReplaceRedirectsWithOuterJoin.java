package org.wikipedia.citolytics.redirects.operators;

import org.apache.flink.api.common.functions.JoinFunction;
import org.wikipedia.citolytics.cpa.types.LinkTuple;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;

public class ReplaceRedirectsWithOuterJoin implements JoinFunction<WikiSimResult, RedirectMapping, WikiSimResult> {
    public int replaceField = 0;
    public int hashField = WikiSimResult.HASH_KEY;
    public int pageAField = WikiSimResult.PAGE_A_KEY;
    public int pageBField = WikiSimResult.PAGE_B_KEY;

    public ReplaceRedirectsWithOuterJoin(int replaceField) {
        this.replaceField = replaceField;
    }

    @Override
    public WikiSimResult join(WikiSimResult record, RedirectMapping redirect) throws Exception {

        if (redirect != null) {
            // replace page in original record
            record.setField(redirect.getTarget(), replaceField);

            // check for alphabetical order (A before B)
            int order = ((String) record.getField(pageAField)).compareTo(
                    (String) record.getField(pageBField));

            // order is wrong (negative)
            if (order > 0) {
                // set correct order
                String tmp = record.getField(pageBField);

                record.setField(record.getField(pageAField), pageBField);
                record.setField(tmp, pageAField);
            }
            // update hash
            record.setField(LinkTuple.getHash((String) record.getField(pageAField), (String) record.getField(pageBField)), hashField);
        }

        return record;
    }
}
