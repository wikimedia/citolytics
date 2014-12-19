package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import de.tuberlin.dima.schubotz.cpa.types.LinkTuple;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;

/**
 * Long, LinkTuple, Long, Integer, Long, Double>
 */
public class WikiSimPlainResult extends Tuple7<Long, String, String, Long, Integer, Long, Double> {
    public static int PAGE1_KEY = 1;
    public static int PAGE2_KEY = 2;
    public static int CPA_KEY = 6;
    public static int COCIT_KEY = 4;

}
