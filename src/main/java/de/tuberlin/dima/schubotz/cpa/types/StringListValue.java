package de.tuberlin.dima.schubotz.cpa.types;

import org.apache.flink.types.ListValue;
import org.apache.flink.types.StringValue;

/**
 * Created by malteschwarzer on 04.12.14.
 */
public class StringListValue extends ListValue<StringValue> {

    public String XtoString() {
        // removes brackets
        return super.toString().substring(1, super.toString().length() - 1);
    }

    public static StringListValue valueOf(String[] array) {
        StringListValue list = new StringListValue();
        for (String s : array) {
            list.add(new StringValue(s));
        }
        return list;
    }

}
