package de.tuberlin.dima.schubotz.cpa.evaluation.utils;

import org.apache.commons.collections.ListUtils;

import java.util.List;

public class EvaluationMeasures {

    public static double getTopKScore(List<String> recommendedResults, List<String> correctResponseList) {
        // topK = correct recommended results / total number of correct results
        if (recommendedResults.size() == 0)
            return 0;

        int subListLength = correctResponseList.size();

        if (recommendedResults.size() < correctResponseList.size()) {
            subListLength = recommendedResults.size();
        }

        // cast to double to receive double result
        return ((double) ListUtils.intersection(correctResponseList, recommendedResults.subList(0, subListLength)).size())
                / ((double) correctResponseList.size());
    }

    public static double getHarmonicReciprocalRank(List<String> recommendedResults, List<String> correctResponseList) {
        double rank = 0;

        if (correctResponseList.size() == 0)
            return 0;

        for (String response : recommendedResults) {
            rank += getHarmonicReciprocalRank(correctResponseList, response);
        }

        return rank / getHarmonicNumber(correctResponseList.size());
    }

    public static double getHarmonicNumber(int n) {
        double sum = 0.0;
        for (int i = 1; i <= n; i++) {
            //sum += 1.0 / i;
            sum += (1.0 / i);
        }
        return sum;
    }

    public static double getHarmonicReciprocalRank(List<String> correctResults, String recomendedResult) {
        int rank = correctResults.indexOf(recomendedResult) + 1;
        double hrr = 0;

        if (rank > 0) {
            hrr = 1.0 / rank;
        }
        return hrr;
    }

    public static double getMeanReciprocalRank(List<String> sortedResults, List<String> correctResponseList) {
        double rank = 0;

        if (correctResponseList.size() == 0)
            return 0;

        for (String correct : correctResponseList) {
            rank += getMeanReciprocalRank(sortedResults, correct);
        }

        return rank / correctResponseList.size();
    }

    public static double getMeanReciprocalRank(List<String> sortedResults, String correctResponse) {
        int rank = sortedResults.indexOf(correctResponse) + 1;
        double mrr = 0;

        if (rank > 0) {
            mrr = 1.0 / rank;
        }
        return mrr;
    }
}
