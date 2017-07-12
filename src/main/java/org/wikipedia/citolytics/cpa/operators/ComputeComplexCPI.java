package org.wikipedia.citolytics.cpa.operators;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.apache.flink.api.common.functions.JoinFunction;
import org.wikipedia.citolytics.cpa.types.Recommendation;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.citolytics.stats.types.ArticleStatsTuple;

/**
 * Recomputes CPI value with script expression, e.g. to use idf.
 *
 * WARNING: This is should not be accessible by any user. The executed JavaScript can access all Java classes
 * and thus hijack your application without limit!
 *
 * Script expressions use String.format() index syntax, where
 *  - original CPI score: x %1$f
 *  - in-links count of recommended article: y %2$d
 *  - total article count: z %3$d
 *
 * Examples:
 *  - idf log: x*log(z/(y+1))         %1$f*Math.log(%3$d/%2$d)
 *  - idf inverse: %1$f/%2$d
 *  - original: %1$f
 *
 */
public class ComputeComplexCPI implements JoinFunction<Recommendation, ArticleStatsTuple, Recommendation> {
    private long articleCount = 0;
    private String cpiExpressionStr = "1";
    private boolean backupRecommendations = false;
//    private Expression cpiExpression;

    public ComputeComplexCPI(long articleCount, String cpiExpressionStr, boolean backupRecommendations) {
        this.articleCount = articleCount;
        this.backupRecommendations = backupRecommendations;

        if (cpiExpressionStr != null && !cpiExpressionStr.isEmpty()) {
            this.cpiExpressionStr = cpiExpressionStr;

//            cpiExpression = new ExpressionBuilder(cpiExpressionStr)
//                    .variables("x", "y", "z")
//                    .build()
//                    .setVariable("z", articleCount);
        }
    }

    @Override
    public Recommendation join(Recommendation rec, ArticleStatsTuple stats) throws Exception {
        if(stats != null) {
            if(backupRecommendations && rec.getScore() < WikiSimConfiguration.BACKUP_RECOMMENDATION_OFFSET) {
                // This is a backup recommendations
                rec.setScore(rec.getScore() / stats.getInLinks());
            } else {
                // This is a normal recommendation
                double cpi = rec.getScore();

                // If backup recommendations are enabled, subtract offset because we want to apply CPI-expression on original values
                if(backupRecommendations)
                    cpi -= WikiSimConfiguration.BACKUP_RECOMMENDATION_OFFSET;

                Expression cpiExpression;
                cpiExpression = new ExpressionBuilder(cpiExpressionStr)
                        .variables("x", "y", "z")
                        .build()
                        .setVariable("z", articleCount)
                        .setVariable("x", cpi)
                        .setVariable("y", stats.getInLinks());
                cpi = cpiExpression.evaluate();

                // Add subtracted offset again
                if(backupRecommendations)
                    cpi += WikiSimConfiguration.BACKUP_RECOMMENDATION_OFFSET;

                rec.setScore(cpi);
            }
        }
        return rec;
    }
}
