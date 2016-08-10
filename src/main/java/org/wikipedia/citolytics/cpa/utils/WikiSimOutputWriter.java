package org.wikipedia.citolytics.cpa.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem;
import org.wikipedia.citolytics.cpa.io.WikiOutputFormat;

@Deprecated
public class WikiSimOutputWriter<T extends Tuple> {
    private String jobName = null;
    private boolean writeAsText = false;
    private int parallelism = -1;

    public WikiSimOutputWriter() {

    }

    public WikiSimOutputWriter(String jobName) {
        this.jobName = jobName;
    }

    public WikiSimOutputWriter<T> setJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    public WikiSimOutputWriter<T> asText() {
        this.writeAsText = true;
        return this;
    }

    public WikiSimOutputWriter<T> setParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public void write(ExecutionEnvironment env, DataSet dataSet, String outputFilename) throws Exception {
        if (outputFilename.equals("print")) {
            dataSet.print();
        } else {
            DataSink sink;

            if (writeAsText) {
                sink = dataSet.writeAsText(outputFilename, FileSystem.WriteMode.OVERWRITE);
            } else {
                sink = dataSet.write(new WikiOutputFormat<T>(outputFilename), outputFilename, FileSystem.WriteMode.OVERWRITE);
            }

            if (parallelism > 0)
                sink.setParallelism(parallelism);

            if (jobName == null)
                env.execute();
            else
                env.execute(jobName);
        }
    }
}
