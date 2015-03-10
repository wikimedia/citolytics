package de.tuberlin.dima.schubotz.cpa.types.list;

import org.apache.flink.types.ListValue;
import org.apache.flink.types.StringValue;

import java.util.ArrayList;
import java.util.List;

public class StringListValue extends ListValue<StringValue> {

    public String XtoString() {
        // removes brackets
        return super.toString().substring(1, super.toString().length() - 1);
    }

    public StringListValue() {
    }

    public static StringListValue valueOf(String[] array) {
        StringListValue list = new StringListValue();
        for (String s : array) {
            list.add(new StringValue(s));
        }
        return list;
    }

    public void add(String s) {
        this.add(new StringValue(s));
    }

    public List<String> asList() {
        List<String> list = new ArrayList<>();
        while (this.listIterator().hasNext()) {
            list.add(this.listIterator().next().getValue());
        }
        return list;
    }

}
