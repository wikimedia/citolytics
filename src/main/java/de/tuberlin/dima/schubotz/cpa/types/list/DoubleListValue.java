package de.tuberlin.dima.schubotz.cpa.types.list;

import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.ListValue;

public class DoubleListValue extends ListValue<DoubleValue> {
    public String toString() {
        // removes brackets
        return StringUtils.join(this, WikiSimConfiguration.csvFieldDelimiter);
    }

    public static DoubleListValue valueOf(double[] array) {
        DoubleListValue list = new DoubleListValue();
        for (double s : array) {
            list.add(new DoubleValue(s));
        }
        return list;
    }
}
