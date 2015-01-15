package de.tuberlin.dima.schubotz.cpa.evaluation.io;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimPlainResult;

public class WikiSimResultInputFormat extends GenericCsvDelimitedInputFormat<WikiSimPlainResult> {
    private String fields = "01101";

    public WikiSimResultInputFormat() {
        //includeFields("0110001000");
        includeFields(fields + "1");
    }

    public WikiSimResultInputFormat setCPAKey(int key) {
        String fields = "01101";
        for (int i = 5; i < key; i++) {
            fields += "0";
        }
        fields += "1";

//        System.out.println(fields);

        includeFields(fields);

        return this;
    }
}
