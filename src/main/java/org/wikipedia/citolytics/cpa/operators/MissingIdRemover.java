package org.wikipedia.citolytics.cpa.operators;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.IdTitleMapping;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;

/**
 * Removes pages without id from WikiSim results
 */
public class MissingIdRemover implements FlatJoinFunction<RecommendationPair, IdTitleMapping, RecommendationPair> {

    private boolean pageA = true;

    public MissingIdRemover(boolean pageA) {
        this.pageA = pageA;
    }

    @Override
    public void join(RecommendationPair recommendationPair, IdTitleMapping mapping, Collector<RecommendationPair> out) throws Exception {
        if (mapping != null) {
            if(pageA) {
                recommendationPair.setPageAId(mapping.getField(IdTitleMapping.ID_KEY));
            } else {
                recommendationPair.setPageBId(mapping.getField(IdTitleMapping.ID_KEY));

            }
            out.collect(recommendationPair);
        }
    }

    /**
     * Removes results with missing ids, i.e. pages that do not exist.
     *
     * @param wikiSimResults Original result tuples
     * @param idTitleMapping Data set with id-title mapping
     * @return Cleaned result set
     */
    public static DataSet<RecommendationPair> removeMissingIds(DataSet<RecommendationPair> wikiSimResults, DataSet<IdTitleMapping> idTitleMapping) throws Exception {
        idTitleMapping.print();

        return wikiSimResults
                // page A
                .leftOuterJoin(idTitleMapping)
                .where(RecommendationPair.PAGE_A_KEY)
                .equalTo(IdTitleMapping.TITLE_KEY)
                .with(new MissingIdRemover(true))
                // page B
                .leftOuterJoin(idTitleMapping)
                .where(RecommendationPair.PAGE_B_KEY)
                .equalTo(IdTitleMapping.TITLE_KEY)
                .with(new MissingIdRemover(false));
    }
}