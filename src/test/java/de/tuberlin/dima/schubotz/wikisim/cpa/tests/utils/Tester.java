package de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.NoSuchElementException;

public class Tester {
    public String resource(String filename) throws FileNotFoundException {
        try {
            return "file://" + getClass().getClassLoader().getResources(filename).nextElement().getPath();
        } catch (NoSuchElementException | IOException e) {
            throw new FileNotFoundException("Test resource not found: " + filename);
        }
    }
}
