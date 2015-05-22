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

    public static DoubleListValue valueOf(String delimitedString, String delimiterPattern) {
        String[] dbs = delimitedString.split(delimiterPattern);
        DoubleListValue list = new DoubleListValue();
        for (String db : dbs) {
            list.add(new DoubleValue(Double.valueOf(db)));
        }
        return list;
    }

    public static DoubleListValue sum(DoubleListValue firstList, DoubleListValue secondList) throws Exception {
        if (firstList.size() != secondList.size()) {
            throw new Exception("Cannot sum lists with different size.");
        }

        int i = 0;
        for (DoubleValue firstValue : firstList) {
            firstList.set(i, new DoubleValue(firstValue.getValue() + secondList.get(i).getValue()));
            i++;
        }

        return firstList;
    }
}
