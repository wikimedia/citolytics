package de.tuberlin.dima.schubotz.wikisim.seealso.types;

import org.apache.flink.api.java.tuple.Tuple11;

import java.util.ArrayList;

/**
 * Created by malteschwarzer on 03/08/15.
 */
public class SeeAlsoEvaluationResult extends Tuple11<String, ArrayList<String>, Integer, WikiSimComparableResultList<Double>, Integer, Double, Double, Double, Integer, Integer, Integer> {
    public SeeAlsoEvaluationResult() {
    }

    public SeeAlsoEvaluationResult(
            String article,
            ArrayList<String> seeAlsoLinks,
            int seeAlsoLinksCount,
            WikiSimComparableResultList<Double> retrievedDocs,
            int retrievedDocsCount,
            double d1,
            double d2,
            double d3,
            int i1,
            int i2,
            int i3) {
        f0 = article;
        f1 = seeAlsoLinks;
        f2 = seeAlsoLinksCount;
        f3 = retrievedDocs;
        f4 = retrievedDocsCount;
        f5 = d1;
        f6 = d2;
        f7 = d3;
        f8 = i1;
        f9 = i2;
        f10 = i3;
    }
}
