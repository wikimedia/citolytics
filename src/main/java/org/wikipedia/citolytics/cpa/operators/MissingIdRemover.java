package org.wikipedia.citolytics.cpa.operators;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.IdTitleMapping;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;

/**
 * Removes pages without id from WikiSim results
 */
public class MissingIdRemover implements FlatJoinFunction<WikiSimResult, IdTitleMapping, WikiSimResult> {

    private boolean pageA = true;

    public MissingIdRemover(boolean pageA) {
        this.pageA = pageA;
    }

    @Override
    public void join(WikiSimResult wikiSimResult, IdTitleMapping mapping, Collector<WikiSimResult> out) throws Exception {
        if (mapping != null) {
            if(pageA) {
                wikiSimResult.setPageAId(mapping.getField(IdTitleMapping.ID_KEY));
            } else {
                wikiSimResult.setPageBId(mapping.getField(IdTitleMapping.ID_KEY));

            }
            out.collect(wikiSimResult);
        }
    }

    /**
     * Removes results with missing ids, i.e. pages that do not exist.
     *
     * @param wikiSimResults Original result tuples
     * @param idTitleMapping Data set with id-title mapping
     * @return Cleaned result set
     */
    public static DataSet<WikiSimResult> removeMissingIds(DataSet<WikiSimResult> wikiSimResults, DataSet<IdTitleMapping> idTitleMapping) {

        return wikiSimResults
                // page A
                .leftOuterJoin(idTitleMapping)
                .where(WikiSimResult.PAGE_A_KEY)
                .equalTo(IdTitleMapping.TITLE_KEY)
                .with(new MissingIdRemover(true))
                // page B
                .leftOuterJoin(idTitleMapping)
                .where(WikiSimResult.PAGE_B_KEY)
                .equalTo(IdTitleMapping.TITLE_KEY)
                .with(new MissingIdRemover(false));
    }
}