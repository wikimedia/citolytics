package org.wikipedia.citolytics.tests.utils;


import org.junit.ComparisonFailure;
import org.wikipedia.citolytics.WikiSimAbstractJob;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class Tester {
    protected WikiSimAbstractJob job;

    protected WikiSimAbstractJob setJob(WikiSimAbstractJob job) {
        this.job = job;
        this.job.enableLocalEnvironment().enableSingleOutputFile();

        return this.job;
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

    public String getFileContents(String fname) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fname);
        Scanner s = new Scanner(is, "UTF-8");
        s.useDelimiter("\\A");
        String out = s.hasNext() ? s.next() : "";
        s.close();
        return out;
    }

}
