package de.tuberlin.dima.schubotz.wikisim.seealso.io;

import de.tuberlin.dima.schubotz.wikisim.seealso.types.MLTResult;

public class MLTResultInputFormat extends GenericCsvDelimitedInputFormat<MLTResult> {
    public MLTResultInputFormat() {
        //includeFields("0110001000");
        includeFields("111");

    }
}
