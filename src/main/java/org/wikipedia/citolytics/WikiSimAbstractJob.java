package org.wikipedia.citolytics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.wikipedia.citolytics.cpa.io.WikiOutputFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * Parent job class
 */
public abstract class WikiSimAbstractJob<T extends Tuple> {
    private int outputParallelism = -1;
    public String[] args;
    protected String jobName;
    public String outputFilename;
    public List<T> output = new ArrayList<>();
    public DataSet<T> result;
    public ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    private boolean writeAsText = false;

    public WikiSimAbstractJob() {
    }

    protected WikiSimAbstractJob setJobName(String name) {
        jobName = name;
        return this;
    }

    private String getJobName() {
        if (jobName == null)
            return this.getClass().getCanonicalName();
        else
            return jobName;
    }

    /**
     * Disables stdout logging
     *
     * @return Job
     */
    public WikiSimAbstractJob verbose() {
        env.getConfig().disableSysoutLogging();
        return this;
    }

    /**
     * Enables local execution environment that can be used for unit testing (Travis CI bugfix).
     *
     * @return Job
     */
    public WikiSimAbstractJob enableLocalEnvironment() {
        Configuration conf = new Configuration();

//        conf.setInteger("taskmanager.network.numberOfBuffers", 16000);
//        conf.setInteger("taskmanager.numberOfTaskSlots", 32);

        env = ExecutionEnvironment.createLocalEnvironment(conf);

        env.setParallelism(1);

        return this;
    }

    public WikiSimAbstractJob setEnvironment(ExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public WikiSimAbstractJob enableTextOutput() {
        writeAsText = true;
        return this;
    }

    public WikiSimAbstractJob enableSingleOutputFile() {
        outputParallelism = 1;
        return this;
    }

    public void start(String args) throws Exception {
        start(args.split(" "));
    }

    public void start(String[] args) throws Exception {
        this.args = args;
        init();
        plan();
        execute();
    }

    /**
     * This method provides the actual job execution plan
     *
     * @throws Exception
     */
    abstract public void plan() throws Exception;

    public void init() {

    }

    private void execute() throws Exception {
        writeOutput();
    }

    /**
     * Write output to CSV file, text file, local collection or print to console.
     *
     * @throws Exception
     */
    private void writeOutput() throws Exception {

        if (result == null) {
            System.err.println("Result data set is not set.");

        } else {

            if (outputFilename == null) {
                throw new Exception("Output filename is not set.");
            } else if (outputFilename.equalsIgnoreCase("print")) {
                result.print();
            } else if (outputFilename.equalsIgnoreCase("local") || outputFilename.equalsIgnoreCase("collect")) {
//                result.output(new LocalCollectionOutputFormat<>(output));
                output = result.collect();
            } else {
                DataSink sink;

                if (writeAsText) {
                    sink = result.writeAsText(outputFilename, FileSystem.WriteMode.OVERWRITE);
                } else {
                    sink = result.write(new WikiOutputFormat<>(outputFilename), outputFilename, FileSystem.WriteMode.OVERWRITE);
                }

                if (outputParallelism > 0)
                    sink.setParallelism(outputParallelism);

                env.execute(getJobName());
            }
        }
    }

    public List<T> getOutput() throws Exception {
        if (output == null) {
            throw new Exception("No output available (output filename=" + outputFilename + ")");
        }
        return this.output;
    }
}
