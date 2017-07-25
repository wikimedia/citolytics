package org.wikipedia.citolytics.clickstream;

import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;
import org.wikipedia.citolytics.clickstream.utils.ClickStreamHelper;

/**
 * Translates click stream data set. Helper job to write intermediate results instead of on-the-fly translation.
 */
public class ClickStreamTranslator extends WikiSimAbstractJob<ClickStreamTuple> {
    @Override
    public void plan() throws Exception {
        setJobName("ClickStreamEvaluation");

        outputFilename = getParams().getRequired("output");
        String inputPath = getParams().getRequired("input");
        String lang = getParams().getRequired("lang");
        String idTitleMappingFilename = getParams().get("id-title-mapping");
        String langLinksInputFilename = getParams().get("langlinks");

        // Load gold standard (include translations with requested, provide id-title-mapping if non-id format is used)
        result = ClickStreamHelper.getTranslatedClickStreamDataSet(env, inputPath, lang,
                        langLinksInputFilename, idTitleMappingFilename);
    }
}
