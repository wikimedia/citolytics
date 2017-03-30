package org.wikipedia.citolytics.multilang;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

public class LangLinksFormat extends DelimitedInputFormat<String> {
    private static final long serialVersionUID = 1L;
    private String charsetName = "UTF-8";

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);
        this.setDelimiter("),");
    }

    @Override
    public String readRecord(String reusable, byte[] bytes, int offset, int numBytes) throws IOException {
        return new String(bytes, offset, numBytes, this.charsetName);
    }
}
