package de.tuberlin.dima.schubotz.cpa.evaluation.types;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * MoreLikeThis result
 *
 * 0: Article
 * 1: Target
 * 2: Score
 */
public class MLTResult extends Tuple3<String, String, Float> {
}
