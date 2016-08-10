package org.wikipedia.citolytics.clickstream.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.wikipedia.citolytics.WikiSimJob;

import java.util.HashMap;

/**
 * Check if
 * a) impressions >= sum(in-clicks)
 * b) impressions >= in-clicks
 * <p/>
 * Outputs all invalid records.
 */
public class ValidateClickStreamData extends WikiSimJob<Tuple3<String, Integer, HashMap<String, Integer>>> {

    public static void main(String[] args) throws Exception {
        new ValidateClickStreamData().start(args);
    }

    public void plan() {

        String clickStreamInputFilename = args[0];
        outputFilename = args[1];

        result = ClickStreamHelper.getRichClickStreamDataSet(env, clickStreamInputFilename)
                .filter(new FilterFunction<Tuple3<String, Integer, HashMap<String, Integer>>>() {
                    @Override
                    public boolean filter(Tuple3<String, Integer, HashMap<String, Integer>> test) throws Exception {
                        int impressions = test.f1;
                        int allClicks = 0;

                        for (int clicks : test.f2.values()) {
                            allClicks += clicks;
                        }

                        // Is record valid?
                        return (allClicks > impressions);
                    }
                });

    }
}
