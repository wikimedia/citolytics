package org.wikipedia.citolytics.cpa.types.list;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.ListValue;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;

public class IntegerListValue extends ListValue<IntValue> {
    public String toString() {
        // removes brackets
        return StringUtils.join(this, WikiSimConfiguration.csvFieldDelimiter);
    }

    public static IntegerListValue valueOf(int[] array) {
        IntegerListValue list = new IntegerListValue();
        for (int s : array) {
            list.add(new IntValue(s));
        }
        return list;
    }
}
