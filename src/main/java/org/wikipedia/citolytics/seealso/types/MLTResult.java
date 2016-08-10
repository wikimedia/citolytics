package org.wikipedia.citolytics.seealso.types;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * MoreLikeThis result
 * <p/>
 * 0: Article
 * 1: Target
 * 2: Score
 */
@Deprecated
public class MLTResult extends Tuple3<String, String, Float> {
}
