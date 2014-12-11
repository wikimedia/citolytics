package de.tuberlin.dima.schubotz.cpa.evaluation.io;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.CPAResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.MLTResult;

public class MLTResultInputFormat extends GenericCsvDelimitedInputFormat<MLTResult> {
    public MLTResultInputFormat() {
        //includeFields("0110001000");
        includeFields("111");

    }
}
