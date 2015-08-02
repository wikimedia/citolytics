package de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils;

import java.io.IOException;

public class Tester {
    public String resource(String filename) throws Exception {
        try {
            return "file://" + getClass().getClassLoader().getResources(filename).nextElement().getPath();
        } catch (IOException e) {
            throw new Exception("Test resource not found: " + filename);
        }
    }
}
