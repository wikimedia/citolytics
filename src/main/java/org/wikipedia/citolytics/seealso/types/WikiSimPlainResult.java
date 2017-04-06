package org.wikipedia.citolytics.seealso.types;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Long, LinkPair, Long, Integer, Long, Int: Min, Int: Max, Double>
 */
@Deprecated
public class WikiSimPlainResult extends Tuple4<String, String, Integer, Double> {
    public static int PAGE1_KEY = 0;
    public static int PAGE2_KEY = 1;
    public static int CPA_KEY = 3;
    public static int COCIT_KEY = 2;

}
