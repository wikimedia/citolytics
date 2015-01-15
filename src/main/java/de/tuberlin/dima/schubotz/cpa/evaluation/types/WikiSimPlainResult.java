package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Long, LinkTuple, Long, Integer, Long, Int: Min, Int: Max, Double>
 */
public class WikiSimPlainResult extends Tuple4<String, String, Integer, Double> {
    public static int PAGE1_KEY = 0;
    public static int PAGE2_KEY = 1;
    public static int CPA_KEY = 3;
    public static int COCIT_KEY = 2;

}
