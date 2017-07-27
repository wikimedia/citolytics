package org.wikipedia.citolytics.cirrussearch.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Generate backup recommendations based on out-links from an article. To closer to the top the out-link is, the
 * better the recommendation is.
 *
 * This is used when CPA cannot generated any recommendations (no in-links available).
 */
public class BackupRecommendationSetExtractor implements FlatMapFunction<String, RecommendationSet> {
    private DocumentProcessor dp = new DocumentProcessor();

    @Override
    public void flatMap(String content, Collector<RecommendationSet> out) throws Exception {

        WikiDocument doc = dp.processDoc(content);

        if (doc == null) return;
        if (doc.getNS() != 0) return; // Skip all namespaces other than main


        RecommendationSet set = new RecommendationSet(doc.getTitle(), doc.getId(), getBackupRecommendations(doc));

        if(set.getResults().size() > 0)
            out.collect(set);
    }

    /**
     * Collect recommendations based on outgoing links from article. Order by position of first occurrence.
     * Use as source for backup recommendations.
     *
     * @param doc Fully processed WikiDocument
     */
    private WikiSimComparableResultList<Double> getBackupRecommendations(WikiDocument doc) throws Exception {
        Set<String> outLinks = new HashSet<>();

        WikiSimComparableResultList<Double> recommendations = new WikiSimComparableResultList<>();

        for(Map.Entry<String, Integer> outLink: doc.getOutLinks()) {
            // Only use first occurrence
            if(outLinks.contains(outLink.getKey()))
                continue;

            // Use inverse link position: The closer to the top, the more relevant the link is.
            // TODO Include multiple occurrences?
            double distance = 1. / ((double) outLink.getValue());

            if(distance > Integer.MAX_VALUE)
                throw new Exception("CPI too large: " + distance);

            recommendations.add(new WikiSimComparableResult<>(outLink.getKey(), distance, -1));

            // Only collect minimum number of backup recommendations
            if(recommendations.size() >= WikiSimConfiguration.BACKUP_RECOMMENDATION_COUNT) {
                break;
            }
        }

        return recommendations;
    }

    public static DataSet<RecommendationSet> getBackupRecommendations(ExecutionEnvironment env, String path) {
        return env.readFile(new WikiDocumentDelimitedInputFormat(), path).flatMap(new BackupRecommendationSetExtractor());
    }
}
