package org.wikipedia.citolytics.seealso.types;

import org.apache.flink.api.java.tuple.Tuple11;

import java.util.Collection;

public class SeeAlsoEvaluationResult extends Tuple11<String, Collection<String>, Integer, WikiSimComparableResultList<Double>, Integer, Double, Double, Double, Integer, Integer, Integer> {
    public SeeAlsoEvaluationResult() {
    }

    public SeeAlsoEvaluationResult(
            String article,
            Collection<String> seeAlsoLinks,
            int seeAlsoLinksCount,
            WikiSimComparableResultList<Double> retrievedDocs,
            int retrievedDocsCount,
            double hrr,
            double topKScore,
            double performanceMeasure,
            int relevantCount1,
            int relevantCount2,
            int relevantCount3) {
        f0 = article;
        f1 = seeAlsoLinks;
        f2 = seeAlsoLinksCount;
        f3 = retrievedDocs;
        f4 = retrievedDocsCount;
        f5 = hrr;
        f6 = topKScore;
        f7 = performanceMeasure;
        f8 = relevantCount1;
        f9 = relevantCount2;
        f10 = relevantCount3;
    }

    public String getArticle() {
        return f0;
    }

    public int getRetrievedDocsCount() {
        return f4;
    }

    public double getHRR() {
        return f5;
    }

    public double getTopKScore() {
        return f6;
    }

    public double getPerformanceMeasure() {
        return f7;
    }

    public int getRelevantCount1() {
        return f8;
    }

    @Override
    public int hashCode() {
        return f0.hashCode() + f1.hashCode() + f3.hashCode();
    }
}
