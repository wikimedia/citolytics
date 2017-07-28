package org.wikipedia.citolytics.edits;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.edits.types.EditEvaluationResult;

/**
 * Offline evaluation of article recommendations based on article edits by Wikipedia users. Authors contribute
 * most likely to article that are related to each other. Therefore, we can utilize edits as gold standard for
 * evaluating recommendations.
 *
 * Approach: Extract article-author pairs, find co-authored articles, order by count.
 *  Result will have article-recommendation-count triples.
 *
 * DataSource: stub-meta-history.xml-dump (available for each language)
 *
 * Problems:
 * - Edits by bots (how to exclude them?, comment contains "bot")
 *
 */
public class EditEvaluation extends WikiSimAbstractJob<EditEvaluationResult> {

    @Override
    public void plan() throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String goldFilename = params.getRequired("gold");
        outputFilename = params.getRequired("output");

        DataSet<RecommendationSet> goldStandard = EditRecommendationExtractor.extractRecommendations(env, goldFilename, null);

        // TODO
    }
}
