package de.tuberlin.dima.schubotz.cpa.evaluation.io;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.CPAResult;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.regex.Pattern;

/**
 * Work-Around of Apache Flink CsvImportFormat: Allows quotes in CSV field data
 */
public class GenericCsvDelimitedInputFormat<OUT extends Tuple> extends DelimitedInputFormat<OUT> {

    private static final long serialVersionUID = 1L;
    private String charsetName = "UTF-8";

    private String rowDelimitter = "\n";
    private String fieldDelimitter = "|";
    private String includedFields = null;
    private int fieldCount = -1;

    public String getCharsetName() {
        return charsetName;
    }

    public GenericCsvDelimitedInputFormat includeFields(String fields) {
        this.includedFields = fields;
        return this;
    }

    public GenericCsvDelimitedInputFormat setRowDelimitter(String delimitter) {
        rowDelimitter = delimitter;
        return this;
    }

    public GenericCsvDelimitedInputFormat setFieldDelimitter(String delimitter) {
        fieldDelimitter = delimitter;
        return this;
    }

    public int getFieldCount() {
        if (fieldCount == -1) {
            if (includedFields == null) {
                fieldCount = new CPAResult().getArity();
            } else {
                fieldCount = 0;
                for (int i = 0; i < includedFields.length(); i++) {
                    if (includedFields.charAt(i) == '1') {
                        fieldCount++;
                    }
                }
            }
        }
        return fieldCount;
    }

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);
        this.setDelimiter(rowDelimitter);
    }

    @Override
    public OUT readRecord(OUT reusable, byte[] bytes, int offset, int numBytes) throws IOException {
        String row = new String(bytes, offset, numBytes, this.charsetName);

        OUT record = reusable;
        String[] fields = row.split(Pattern.quote(fieldDelimitter));
//        System.out.println(Arrays.asList(fields));

        int f = 0;

        for (int k = 0; k < fields.length; k++) {
            if (isIncludedField(k)) {
                Class clazz = (Class) ((ParameterizedType) record.getClass().getGenericSuperclass()).getActualTypeArguments()[f];

                // Cast to Tuple types
                if (clazz == Integer.class) {
                    record.setField(Integer.valueOf(fields[k]), f);
                } else if (clazz == Double.class) {
                    record.setField(Double.valueOf(fields[k]), f);
                } else if (clazz == Float.class) {
                    record.setField(Float.valueOf(fields[k]), f);
                } else if (clazz == Long.class) {
                    record.setField(Long.valueOf(fields[k]), f);
                } else {
                    record.setField(clazz.cast(fields[k]), f);
                }
                f++;
            }
        }

//        } else {
//            throw new ParseException("Field count does not match expected count. Expected: " + getFieldCount() + "; Fields: " + fields.length);
//        }

        return record;
    }

    private boolean isIncludedField(int index) {
        if (includedFields != null && includedFields.charAt(index) == '0') {
            return false;
        } else {
            return true;
        }
    }
}
