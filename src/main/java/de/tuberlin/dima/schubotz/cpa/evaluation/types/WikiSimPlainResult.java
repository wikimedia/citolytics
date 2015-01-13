package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import org.apache.flink.api.java.tuple.Tuple9;

/**
 * Long, LinkTuple, Long, Integer, Long, Int: Min, Int: Max, Double>
 */
public class WikiSimPlainResult extends Tuple9<Long, String, String, Long, Integer, Long, Integer, Integer, Double> {
    public static int PAGE1_KEY = 1;
    public static int PAGE2_KEY = 2;
    public static int CPA_KEY = 8;
    public static int COCIT_KEY = 4;

}
