package org.wikipedia.citolytics.edits.types;

import org.apache.flink.api.java.tuple.Tuple4;

import java.util.List;

public class EditEvaluationResult extends Tuple4<String, List<String>, List<String>, Integer> {
    public EditEvaluationResult() {
        // Flink requires empty constructor
    }

    public EditEvaluationResult(String article, List<String> goldStandard, List<String> recommendations, int relevant) {
        f0 = article;
        f1 = goldStandard;
        f2 = recommendations;
        f3 = relevant;
    }
}
