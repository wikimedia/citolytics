package de.tuberlin.dima.schubotz.cpa.redirects;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

@RichGroupReduceFunction.Combinable
public class ReduceResults extends RichGroupReduceFunction<WikiSimRedirectsResult2, WikiSimRedirectsResult2> {
    public static final String delimiterPattern = Pattern.quote("|");

    @Override
    public void combine(Iterable<WikiSimRedirectsResult2> in, Collector<WikiSimRedirectsResult2> out) throws Exception {
        internalReduce(in, out);
    }

    @Override
    public void reduce(Iterable<WikiSimRedirectsResult2> in, Collector<WikiSimRedirectsResult2> out) throws Exception {
        internalReduce(in, out);
    }

    public void internalReduce(Iterable<WikiSimRedirectsResult2> in, Collector<WikiSimRedirectsResult2> out) throws Exception {
        Iterator<WikiSimRedirectsResult2> iterator = in.iterator();
        WikiSimRedirectsResult2 reducedRecord = null;

        List<String> delimitedStrings = new ArrayList<>();

        while (iterator.hasNext()) {
            WikiSimRedirectsResult2 currentRecord = iterator.next();

            delimitedStrings.add(currentRecord.f3);
            // init
            if (reducedRecord == null) {
                reducedRecord = currentRecord;
            }
        }

        if (delimitedStrings.size() > 1) {
            // fixed values for distance and count
            long distance = 0;
            int count = 0;
            Double[] cpa = null;

            for (String delimitedString : delimitedStrings) {
                String[] cols = delimitedString.split(delimiterPattern);

                // initialize cpa array
                if (cpa == null) {
                    cpa = new Double[cols.length - 2];
                    for (int i = 0; i < cpa.length; i++) {
                        cpa[i] = new Double(0);
                    }
                }

                if (cols.length != cpa.length + 2)
                    throw new Exception("Cannot sum results with different column length. Old = " + cols.length + "; Current = " + (2 + cpa.length));

                distance += Long.valueOf(cols[0]);
                count += Integer.valueOf(cols[1]);

                for (int i = 0; i < cpa.length; i++) {
                    cpa[i] += Double.valueOf(cols[i + 2]);
                }
            }

            // back to String
            reducedRecord.setField(distance + "|" + count + "|" + StringUtils.join(cpa, '|'), 3);
        }
        out.collect(reducedRecord);
    }
}
