package org.wikipedia.citolytics.seealso.io;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Work-Around of Apache Flink CsvImportFormat: Allows quotes in CSV field data
 */
public class GenericCsvDelimitedInputFormat<OUT extends Tuple> extends DelimitedInputFormat<OUT> {

    private static final long serialVersionUID = 1L;
    protected String charsetName = "UTF-8";

    protected String rowDelimitter = "\n";
    protected String fieldDelimitter = "|";
    protected String includedFields = null;
    protected int fieldCount = -1;

    public String getCharsetName() {
        return charsetName;
    }

    public GenericCsvDelimitedInputFormat includeFields(String fields) {
        this.includedFields = fields;
        return this;
    }

    public String getIncludedFields() {
        return includedFields;
    }

    public GenericCsvDelimitedInputFormat setRowDelimitter(String delimitter) {
        rowDelimitter = delimitter;
        return this;
    }

    public GenericCsvDelimitedInputFormat setFieldDelimitter(String delimitter) {
        fieldDelimitter = delimitter;
        return this;
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
            try {
                if (isIncludedField(k) && f < record.getArity()) {
                    Class clazz = (Class) ((ParameterizedType) record.getClass().getGenericSuperclass()).getActualTypeArguments()[f];

//                    System.out.println(f + " - " + k + ": " + fields[k] + " -> " + clazz.getCanonicalName());

                    // Cast to Tuple types
                    if (clazz == Integer.class) {
                        record.setField(Integer.valueOf(fields[k]), f);
                    } else if (clazz == Double.class) {
                        record.setField(Double.valueOf(fields[k]), f);
                    } else if (clazz == Float.class) {
                        record.setField(Float.valueOf(fields[k]), f);
                    } else if (clazz == Long.class) {
                        record.setField(Long.valueOf(fields[k]), f);
                    } else if (clazz == String.class) {
                        record.setField(String.valueOf(fields[k]), f);
                    } else {
                        try {
                            record.setField(clazz.cast(fields[k]), f);
                        } catch (ClassCastException e) {
                            throw new IOException("Field type is not supported. Field: " + k);
                        }
                    }
                    f++;
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

    protected boolean isIncludedField(int index) {
        if (includedFields != null && index < includedFields.length() && includedFields.charAt(index) == '0') {
            return false;
        } else {
            return true;
        }
    }
}
