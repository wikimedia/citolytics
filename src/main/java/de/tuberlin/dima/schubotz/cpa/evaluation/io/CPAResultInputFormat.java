package de.tuberlin.dima.schubotz.cpa.evaluation.io;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.CPAResult;

public class CPAResultInputFormat extends GenericCsvDelimitedInputFormat<CPAResult> {
    public CPAResultInputFormat() {
        //includeFields("0110001000");
        includeFields("111");

    }
}
