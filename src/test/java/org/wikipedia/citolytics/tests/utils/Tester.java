package org.wikipedia.citolytics.tests.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class Tester {

    public String input(String filename) throws FileNotFoundException {
        // TODO Check if not empty
        return resource(filename);
    }

    public String output(String filename) throws FileNotFoundException {
        // TODO Check if empty
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
