package org.wikipedia.citolytics.cpa.operators;

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
 *  - original CPI score: %1$f
 *  - in-links count of recommended article: %2$d
 *  - total article count: %3$d
 *
 * Examples:
 *  - idf log: %1$f*Math.log(%3$d/%2$d)
 *  - idf inverse: %1$f/%2$d
 *  - original: %1$f
 *
 */
public class ComputeComplexCPI implements JoinFunction<Recommendation, ArticleStatsTuple, Recommendation> {
    private long articleCount = 0;
    private String cpiExpr = "%1$d";

    public ComputeComplexCPI(long articleCount, String cpiExpr) {
        this.articleCount = articleCount;

        if (cpiExpr != null && !cpiExpr.isEmpty()) {
            this.cpiExpr = cpiExpr;
        }
    }

    @Override
    public Recommendation join(Recommendation rec, ArticleStatsTuple stats) throws Exception {
        if(stats != null) {

//            try {

//                ScriptEngineManager mgr = new ScriptEngineManager();
//                ScriptEngine engine = mgr.getEngineByName("JavaScript");
//                double cpi = (double) engine.eval(String.format(cpiExpr, rec.getScore(), stats.getInLinks(), articleCount));
                double cpi = rec.getScore() * Math.log( articleCount / stats.getInLinks() );
                rec.setScore(cpi);

//            } catch(ScriptException | IllegalFormatConversionException e) {
//                throw new Exception("Cannot evaluate CPI script expression: " + cpiExpr + "; Exception: " + e.getMessage());
//            }
//            System.out.println(">>> " + rec.getRecommendationTitle() + "\t count= " + articleCount + "\t  idf=" + idf + "\t new score=" + rec.getScore() + "\t old=" + oldScore);

        }
        return rec;
    }
}
