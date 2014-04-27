package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class LinkDetectionTest {
    private String getFileContents(String fname) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fname);
        Scanner s = new Scanner(is, "UTF-8");
        s.useDelimiter("\\A");
        String out = s.hasNext() ? s.next() : "";
        s.close();
        return out;
    }

    @Test
    public void testLinks() {
        WikiDocument doc = new WikiDocument();
        doc.setText(getFileContents("linkTest.wmd"));
        List<Map.Entry<String, Integer>> links = doc.getOutLinks();
        for (Map.Entry<String, Integer> link : links) {
            System.out.println(link.getKey());
        }

    }

}
