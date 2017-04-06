package org.wikipedia.citolytics.redirects.operators;

import org.apache.flink.api.common.functions.JoinFunction;
import org.wikipedia.citolytics.cpa.types.LinkTuple;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;

public class ReplaceRedirectsWithOuterJoin implements JoinFunction<RecommendationPair, RedirectMapping, RecommendationPair> {
    public int replaceField = 0;
    public int hashField = RecommendationPair.HASH_KEY;
    public int pageAField = RecommendationPair.PAGE_A_KEY;
    public int pageBField = RecommendationPair.PAGE_B_KEY;

    public ReplaceRedirectsWithOuterJoin(int replaceField) {
        this.replaceField = replaceField;
    }

    @Override
    public RecommendationPair join(RecommendationPair record, RedirectMapping redirect) throws Exception {

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
