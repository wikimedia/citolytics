package org.wikipedia.citolytics.cpa.operators;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.apache.flink.api.common.functions.JoinFunction;
import org.wikipedia.citolytics.cpa.types.Recommendation;
import org.wikipedia.citolytics.stats.ArticleStatsTuple;

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
//    private Expression cpiExpression;

    public ComputeComplexCPI(long articleCount, String cpiExpressionStr) {
        this.articleCount = articleCount;

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
            double cpi;
            Expression cpiExpression;
            cpiExpression = new ExpressionBuilder(cpiExpressionStr)
                    .variables("x", "y", "z")
                    .build()
                    .setVariable("z", articleCount)
                    .setVariable("x", rec.getScore())
                    .setVariable("y", stats.getInLinks());
            cpi = cpiExpression.evaluate();

//            if(cpiExpression == null) {
//                cpi = 1;
//            } else{
//                Expression e = cpiExpression
//                        .setVariable("x", rec.getScore())
//                        .setVariable("y", stats.getInLinks());
//
//                cpi = e.evaluate();
//            }


//            try {

//                ScriptEngineManager mgr = new ScriptEngineManager();
//                ScriptEngine engine = mgr.getEngineByName("JavaScript");
//                double cpi = (double) engine.eval(String.format(cpiExpressionStr, rec.getScore(), stats.getInLinks(), articleCount));
//                double cpi = rec.getScore() * Math.log( articleCount / (stats.getInLinks() + 1) );
                rec.setScore(cpi);

//            } catch(ScriptException | IllegalFormatConversionException e) {
//                throw new Exception("Cannot evaluate CPI script expression: " + cpiExpressionStr + "; Exception: " + e.getMessage());
//            }
//            System.out.println(">>> " + rec.getRecommendationTitle() + "\t count= " + articleCount + "\t  idf=" + idf + "\t new score=" + rec.getScore() + "\t old=" + oldScore);

        }
        return rec;
    }
}
