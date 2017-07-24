package org.wikipedia.citolytics.seealso.types;

/**
 * Interface for evaluations results ("See also" and click streams)
 */
public interface EvaluationResult {
    String getTopRecommendations();

    int getRecommendationsCount();
}
