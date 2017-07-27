package org.wikipedia.citolytics.cirrussearch.operators;

import org.apache.flink.api.common.functions.JoinFunction;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

import java.util.Collections;

/**
 * Merges original and backup recommendation sets
 */
public class BackupRecommendationSetMerger implements JoinFunction<RecommendationSet, RecommendationSet, RecommendationSet> {
    private int topK;

    public BackupRecommendationSetMerger(int topK) {
        this.topK = topK;
    }

    @Override
    public RecommendationSet join(RecommendationSet originalRecs, RecommendationSet backupRecs) throws Exception {

        // First NULL cases
        if (originalRecs == null) {
            return backupRecs;
        } else if (backupRecs == null) {
            return originalRecs;
        } else if (originalRecs.getResults().size() >= topK) {
            // Original has enough results
            return originalRecs;
        } else {
            // Merge
            WikiSimComparableResultList<Double> recommendations = new WikiSimComparableResultList<>();

            // Add original recommendations with offset
            for (WikiSimComparableResult<Double> item : originalRecs.getResults()) {
                if(item.getSortField1() + WikiSimConfiguration.BACKUP_RECOMMENDATION_OFFSET > Integer.MAX_VALUE) {
                    throw new Exception("CPI too large. Original + Offset: " + item);
                }
                recommendations.add(new WikiSimComparableResult<>(
                        item.getName(),
                        item.getSortField1() + WikiSimConfiguration.BACKUP_RECOMMENDATION_OFFSET,
                        item.getId()
                ));
            }

            // Sort backup first (descending)
            Collections.sort(backupRecs.getResults());
            Collections.reverse(backupRecs.getResults());

            for (WikiSimComparableResult<Double> item : backupRecs.getResults()) {
                if(item.getSortField1() > Integer.MAX_VALUE)
                    throw new Exception("CPI too large. Backup recommendation: " + item);
                recommendations.add(item);

                if (recommendations.size() >= topK) // Only until limit is reached
                    break;
            }

            originalRecs.setResults(recommendations);

            return originalRecs;
        }
    }
}
