package de.tuberlin.dima.schubotz.wikisim.seealso.io;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.com.google.common.primitives.Ints;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by malteschwarzer on 28.02.15.
 */
public class CPAInputFormat extends GenericCsvDelimitedInputFormat<Tuple3<String, String, Double>> {

    // 01101
    protected int page1_field = 1;
    protected int page2_field = 2;
    protected int score_field = 8;

    public CPAInputFormat enableTwin() {
        int tmp_page1_field = page2_field;

        page2_field = page1_field;
        page1_field = tmp_page1_field;

        return this;
    }

    @Override
    public Tuple3<String, String, Double> readRecord(Tuple3<String, String, Double> reusable, byte[] bytes, int offset, int numBytes) throws IOException {
        String row = new String(bytes, offset, numBytes, this.charsetName);

        Tuple3<String, String, Double> record = reusable;
        String[] fields = row.split(Pattern.quote(fieldDelimitter));
//        System.out.println(Arrays.asList(fields));

        int f = 0;
        for (int k = 0; k < Ints.max(new int[]{page1_field, page2_field, score_field}); k++) {
            try {

                if (k == page1_field) {
                    record.setField(String.valueOf(fields[k]), 0);
                } else if (k == page2_field) {
                    record.setField(String.valueOf(fields[k]), 1);
                } else if (k == score_field) {
                    record.setField(Double.valueOf(fields[k]), 2);

                }
            } catch (StringIndexOutOfBoundsException e) {
                throw new IOException("Cannot read line: " + Arrays.toString(fields) + "\nField: " + k + "; Include fields: " + includedFields + "\n" + e.getMessage());

            }
        }

//        } else {
//            throw new ParseException("Field count does not match expected count. Expected: " + getFieldCount() + "; Fields: " + fields.length);
//        }

        return record;
    }
}
