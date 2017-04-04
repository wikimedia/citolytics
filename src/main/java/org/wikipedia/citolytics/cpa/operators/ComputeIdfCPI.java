package org.wikipedia.citolytics.cpa.operators;

import org.apache.flink.api.common.functions.JoinFunction;
import org.wikipedia.citolytics.cpa.types.WikiSimRecommendation;
import org.wikipedia.citolytics.stats.ArticleStatsTuple;


public class ComputeIdfCPI implements JoinFunction<WikiSimRecommendation, ArticleStatsTuple, WikiSimRecommendation> {
    private long articleCount = 0;

    public ComputeIdfCPI(long articleCount) {
        this.articleCount = articleCount;
    }

    @Override
    public WikiSimRecommendation join(WikiSimRecommendation rec, ArticleStatsTuple stats) throws Exception {
        // TODO use score * log(totalArticles / inLinks)
        if(stats != null) {
//            double idf = Math.log(articleCount * stats.getInLinks());
            double oldScore = rec.getScore();
            double idf = 1. / stats.getInLinks();

            rec.setScore(rec.getScore() * idf);

//            System.out.println(">>> " + rec.getRecommendationTitle() + "\t count= " + articleCount + "\t  idf=" + idf + "\t new score=" + rec.getScore() + "\t old=" + oldScore);

        }
        return rec;
    }
}
