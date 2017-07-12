package org.wikipedia.citolytics.tests.utils;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.junit.ComparisonFailure;
import org.wikipedia.citolytics.WikiSimAbstractJob;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class Tester {
    protected WikiSimAbstractJob job;
    protected String fixture;

    protected WikiSimAbstractJob setJob(WikiSimAbstractJob job) {
        this.job = job;
        this.job.enableTestEnvironment();

        return this.job;
    }

    protected String getJobOutputAsString() throws Exception {
        return getJobOutputAsString(job);
    }

    protected String getJobOutputAsString(WikiSimAbstractJob job) throws Exception {

        List<String> output = new ArrayList<>();
        for(Object item: job.getOutput()) {
            output.add(item.toString());
        }
        Collections.sort(output);

        String actualOutput = String.join("\n", output);

        return actualOutput;
    }

    protected void assertJobOutputStringWithResource(WikiSimAbstractJob job, String resourcePath, String message) throws Exception {
        String actualOutput = getJobOutputAsString(job);
        String expectedOutput = FileUtils.readFileToString(new File(getClass().getClassLoader().getResources(resourcePath).nextElement().getPath()));

        assertEquals(message, expectedOutput, actualOutput);
//        assertEquals(message, getFileContents(getExpectedOutputPath()), actualOutput);

    }

    protected void assertTestResources(String message, String expectedPath, String actualPath) throws Exception {

        assertEquals(message, getResourceContents(expectedPath, true),  getResourceContents(actualPath, true));
    }

    protected void assertJobOutputJSONWithResource(WikiSimAbstractJob job, String resourcePath, String message) throws Exception {
        String[] actualOutput = getJobOutputAsString(job).split("\n");
        String[] expectedOutput = FileUtils.readFileToString(new File(getClass().getClassLoader().getResources(resourcePath).nextElement().getPath())).split("\n");

        assertEquals("Output length differs", expectedOutput.length, actualOutput.length);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);

        for(int i=0; i < actualOutput.length; i++) {
            JsonNode actualObj = mapper.readTree(actualOutput[i].substring(1, actualOutput[i].length() - 1));
            JsonNode expectedObj = mapper.readTree(expectedOutput[i].substring(1, expectedOutput[i].length() - 1));

            assertEquals(message, expectedObj, actualObj);
        }

//        assertEquals(message, expectedOutput, actualOutput);
//        assertEquals(message, getFileContents(getExpectedOutputPath()), actualOutput);

    }

    protected void assertJobOutputStringWithFixture(WikiSimAbstractJob job, String message) throws Exception {
        assertJobOutputStringWithResource(job, getExpectedOutputPath(), message);
    }

    protected void assertJobOutputJSONWithFixture(WikiSimAbstractJob job, String message) throws Exception {
        assertJobOutputJSONWithResource(job, getExpectedOutputPath(), message);
    }

    protected void assertJobOutput(WikiSimAbstractJob job, ArrayList needles) throws Exception {
        int found = 0;
        List<Object> invalid = new ArrayList<>();
        for(Object needle: needles) {
            if(job.getOutput().contains(needle)) {
                found++;
            } else {
                invalid.add(needle);
            }
        }

        if(needles.size() != found) {
            throw new ComparisonFailure("Mssing needles! Invalid records: " + invalid, String.valueOf(needles.size()), String.valueOf(found));
        }
    }

    public String input(String filename) throws FileNotFoundException {
        // TODO Check if not empty
        return resource(filename);
    }

    public String output(String filename) throws FileNotFoundException {
        // TODO Check if empty
        return resource(filename);
    }

    public String resource(String filename, boolean testClassDirectory) throws FileNotFoundException {
        if(testClassDirectory) {
            filename = getClass().getSimpleName() + "/" + filename;
        }
        return resource(filename);
    }

    public String resource(String filename) throws FileNotFoundException {

        try {
            return "file://" + getClass().getClassLoader().getResources(filename).nextElement().getPath();
        } catch (NoSuchElementException | IOException e) {
            throw new FileNotFoundException("Test resource not found: " + filename);
        }
    }

    public String getResourceContents(String filename, boolean testClassDirectory) throws FileNotFoundException {
        if(testClassDirectory) {
            filename = getClass().getSimpleName() + "/" + filename;
        }

        try {
            filename = getClass().getClassLoader().getResources(filename).nextElement().getPath();
            return FileUtils.readFileToString(new File(filename));
        } catch (NoSuchElementException | IOException e) {
            throw new FileNotFoundException("Test resource not found: " + filename);
        }

    }

    public String getFileContents(String fname) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fname);
        Scanner s = new Scanner(is, "UTF-8");
        s.useDelimiter("\\A");
        String out = s.hasNext() ? s.next() : "";
        s.close();
        return out;
    }

    protected String getInputPath() throws FileNotFoundException {
        return resource(getClass().getSimpleName() + "/" + fixture + ".input");
    }

    protected String getExpectedOutputPath() throws FileNotFoundException {
//        return resource(getClass().getSimpleName() + "/" + fixture + ".expected");
        return getClass().getSimpleName() + "/" + fixture + ".expected";

    }

    protected ExecutionEnvironment getSilentEnv() {
        ExecutionEnvironment env = new LocalEnvironment();
        env.getConfig().disableSysoutLogging();

        return env;
    }
}
