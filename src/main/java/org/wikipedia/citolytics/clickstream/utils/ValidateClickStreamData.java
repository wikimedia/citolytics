package org.wikipedia.citolytics.clickstream.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;

/**
 * Check if
 * a) impressions >= sum(in-clicks)
 * b) impressions >= in-clicks
 * <p/>
 * Outputs all invalid records.
 */
public class ValidateClickStreamData extends WikiSimAbstractJob<ClickStreamTuple> {

    public static void main(String[] args) throws Exception {
        new ValidateClickStreamData().start(args);
    }

    public void plan() throws Exception {

        String clickStreamInputFilename = args[0];
        outputFilename = args[1];

        result = ClickStreamHelper.getClickStreamDataSet(env, clickStreamInputFilename)
                .filter(new FilterFunction<ClickStreamTuple>() {
                    @Override
                    public boolean filter(ClickStreamTuple test) throws Exception {
                        int impressions = test.getImpressions();
                        int allClicks = 0;

                        for (int clicks : test.getOutClicks().values()) {
                            allClicks += clicks;
                        }

                        // Is record valid?
                        return (allClicks > impressions);
                    }
                });

    }
}
