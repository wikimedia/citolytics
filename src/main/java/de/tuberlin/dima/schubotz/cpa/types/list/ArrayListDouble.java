package de.tuberlin.dima.schubotz.cpa.types.list;

import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;

/**
 * Overwrites toString() of ArrayList for CsvOutput
 * <p/>
 * + Better performance than DoubleListValue
 */
public class ArrayListDouble extends ArrayList<Double> {
    @Override
    public String toString() {
        // removes brackets
        return StringUtils.join(this, WikiSimConfiguration.csvFieldDelimiter);
    }

    public static ArrayList<Double> valueOf(String delimitedString, String delimiterPattern) {
        ArrayListDouble list = new ArrayListDouble();
        String[] dbs = delimitedString.split(delimiterPattern);
        for (String db : dbs) {
            list.add(Double.valueOf(db));
        }
        return list;
    }
}
