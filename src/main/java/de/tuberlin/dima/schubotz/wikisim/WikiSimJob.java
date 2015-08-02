package de.tuberlin.dima.schubotz.wikisim;

import de.tuberlin.dima.schubotz.wikisim.cpa.io.WikiOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * Parent job class
 */
public abstract class WikiSimJob<T extends Tuple> {
    private int outputParallelism = -1;
    public String[] args;
    public String jobName;
    public String outputFilename;
    public List<T> output = new ArrayList<>();
    public DataSet<T> result;
    public final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    private boolean writeAsText = false;

    public WikiSimJob() {
    }

    public WikiSimJob setJobName(String name) {
        jobName = name;
        return this;
    }

    public String getJobName() {
        if (jobName == null)
            return this.getClass().getCanonicalName();
        else
            return jobName;
    }

    public WikiSimJob enableTextOutput() {
        writeAsText = true;
        return this;
    }

    public WikiSimJob enableSingleOutputFile() {
        outputParallelism = 1;
        return this;
    }

    public void start(String[] args) throws Exception {
        this.args = args;
        init();
        plan();
        execute();
    }

    abstract public void plan();

    public void init() {

    }

    public void execute() throws Exception {
        writeOutput();
    }

    /**
     * Write output to CSV file, text file, local collection or print to console.
     *
     * @throws Exception
     */
    public void writeOutput() throws Exception {

        if (outputFilename.equalsIgnoreCase("print")) {
            result.print();
        } else {
            if (outputFilename.equalsIgnoreCase("local")) {
                result.output(new LocalCollectionOutputFormat<>(output));
            } else {
                DataSink sink;

                if (writeAsText) {
                    sink = result.writeAsText(outputFilename, FileSystem.WriteMode.OVERWRITE);
                } else {
                    sink = result.write(new WikiOutputFormat<T>(outputFilename), outputFilename, FileSystem.WriteMode.OVERWRITE);
                }

                if (outputParallelism > 0)
                    sink.setParallelism(outputParallelism);
            }

            env.execute(getJobName());
        }
    }
}
