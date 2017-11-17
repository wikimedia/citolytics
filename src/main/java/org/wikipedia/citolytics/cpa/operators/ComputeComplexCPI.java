package org.wikipedia.citolytics.cpa.operators;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.log4j.Logger;
import org.wikipedia.citolytics.cpa.types.Recommendation;
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
 *  - Okapi BM25: x*log((z-y+0.5)/(y+0.5))
 *
 */
public class ComputeComplexCPI implements JoinFunction<Recommendation, ArticleStatsTuple, Recommendation> {
    private static Logger LOG = Logger.getLogger(ComputeComplexCPI.class);

    private long articleCount = 0;
    private String cpiExpressionStr = "1"; // default: Co-Citation expression

    public ComputeComplexCPI(long articleCount, String cpiExpressionStr) throws Exception {
        this.articleCount = articleCount;

        if (articleCount < 1) // This should normally not happen
            throw new Exception("Article count needs to be >= 1");

        if (cpiExpressionStr == null || cpiExpressionStr.isEmpty()) {
            throw new Exception("CPI expression string is not set.");
        } else {
            this.cpiExpressionStr = cpiExpressionStr;
        }
    }

    @Override
    public Recommendation join(Recommendation rec, ArticleStatsTuple stats) throws Exception {
        if(stats != null) {
            if(stats.getInLinks() < 1) {
                // This should normally not happen (if stats records do not have resolved redirects)
                LOG.warn("Recommendation does not have any in-links: " + rec + "; stats: " + stats);
                stats.setInLinks(1);
            }

            double cpi = rec.getScore();

            // Initialize ExpressionBuilder in join method (non-serializable)
            Expression cpiExpression;
            cpiExpression = new ExpressionBuilder(cpiExpressionStr)
                    .variables("x", "y", "z")
                    .build()
                    .setVariable("z", articleCount)
                    .setVariable("x", cpi)
                    .setVariable("y", stats.getInLinks());
            cpi = cpiExpression.evaluate();

            // Test for too large values
            if(cpi > Integer.MAX_VALUE) {
                throw new Exception("Too large CPI score: CPI > Interger.MAX_VALUE: " + cpi + " (before: "
                        + rec.getScore() + "; expr=" + cpiExpressionStr + "; article count="
                        + articleCount + "; in-links=" + stats.getInLinks());
            }

            rec.setScore(cpi);

        } else {
//            throw new Exception("Recommendation does not have stats records: " + rec);
            LOG.warn("Recommendation does not have a stats record: " + rec);
        }
        return rec;
    }
}
