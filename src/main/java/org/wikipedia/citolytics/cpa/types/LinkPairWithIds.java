package org.wikipedia.citolytics.cpa.types;

import org.apache.flink.api.java.tuple.Tuple4;

public class LinkPairWithIds extends Tuple4<String, String, Integer, Integer> {
    public final static int PAGE_A_TITLE_KEY = 0;
    public final static int PAGE_B_TITLE_KEY = 1;
    public final static int PAGE_A_ID_KEY = 2;
    public final static int PAGE_B_ID_KEY = 3;

    public LinkPairWithIds() {

    }
    public LinkPairWithIds(String titleA, String titleB, int idA, int idB) {
        setField(titleA, PAGE_A_TITLE_KEY);
        setField(titleB, PAGE_B_TITLE_KEY);
        setField(idA, PAGE_A_ID_KEY);
        setField(idB, PAGE_B_ID_KEY);
    }

}
