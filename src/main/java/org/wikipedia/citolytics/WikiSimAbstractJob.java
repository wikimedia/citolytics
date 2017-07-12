package org.wikipedia.citolytics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.wikipedia.citolytics.cpa.io.WikiOutputFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parent job class
 */
public abstract class WikiSimAbstractJob<T extends Tuple> {
    private int outputParallelism = -1;
    public boolean disableOutput = false;
    public String[] args;
    private ParameterTool params;

    protected String jobName;
    public String outputFilename;
    public List<T> output = new ArrayList<>();
    public DataSet<T> result;
    public ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    private boolean writeAsText = false;

    protected WikiSimAbstractJob() {

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
    public WikiSimAbstractJob silent() {
//        env.getConfig().disable
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

        conf.setInteger("taskmanager.network.numberOfBuffers", 1024);
        conf.setInteger("taskmanager.numberOfTaskSlots", 32);

        env = ExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(1);

        return this;
    }

    /**
     * Enables execution environment that is used for unit testing (local + silent).
     *
     * @return
     */
    public WikiSimAbstractJob enableTestEnvironment() {
        enableLocalEnvironment();
        enableSingleOutputFile();
        silent();

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
        // From https://stackoverflow.com/questions/366202/regex-for-splitting-a-string-using-space-when-not-surrounded-by-single-or-double
        List<String> argsList = new ArrayList<String>();
        Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
        Matcher regexMatcher = regex.matcher(args);
        while (regexMatcher.find()) {
            if (regexMatcher.group(1) != null) {
                // Add double-quoted string without the quotes
                argsList.add(regexMatcher.group(1));
            } else if (regexMatcher.group(2) != null) {
                // Add single-quoted string without the quotes
                argsList.add(regexMatcher.group(2));
            } else {
                // Add unquoted word
                argsList.add(regexMatcher.group());
            }
        }

        String[] argsArr = new String[argsList.size()];
        argsList.toArray(argsArr);

        start(argsArr);
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
        if(!disableOutput)
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

    public ParameterTool getParams() {
        if(params == null) {
            params = ParameterTool.fromArgs(args);
        }
        return params;
    }
}
