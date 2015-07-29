package de.tuberlin.dima.schubotz.wikisim.cpa.io;

import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.Path;

/**
 * Forces output to be UTF-8 encoded.
 *
 * @param <T> Output Tuple
 */
public class WikiOutputFormat<T extends Tuple> extends CsvOutputFormat<T> {

    public WikiOutputFormat(String outputPath) {

        super(new Path(outputPath), "\n", "|");
        setCharsetName("UTF-8");
    }
}
