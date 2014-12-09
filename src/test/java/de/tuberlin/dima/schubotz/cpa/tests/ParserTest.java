package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.contracts.DocumentProcessor;
import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
import org.apache.flink.util.Collector;
import org.junit.Test;
import sun.jvm.hotspot.utilities.Assert;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;

/**
 * namespaces
 * http://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces
 */

public class ParserTest {
    List<String> testDocs = new ArrayList<String>(Arrays.asList(""));

    private String getFileContents(String fname) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fname);
        Scanner s = new Scanner(is, "UTF-8");
        s.useDelimiter("\\A");
        String out = s.hasNext() ? s.next() : "";
        s.close();
        return out;
    }

    @Test
    public void testHtmlLinks() {
        WikiDocument doc = new WikiDocument();
        doc.setText(getFileContents("wikiTalkPage.xml"));
        List<Map.Entry<String, Integer>> links = doc.getOutLinks();
        for (Map.Entry<String, Integer> link : links) {
            System.out.println(link.getKey() + ";" + link.getValue());
        }

    }


    @Test
    public void testLinks() {
        WikiDocument doc = new WikiDocument();
        doc.setText(getFileContents("wikiSeeAlso.xml"));
        List<Map.Entry<String, Integer>> links = doc.getOutLinks();
        for (Map.Entry<String, Integer> link : links) {
            System.out.println(link.getKey() + ";" + link.getValue());
        }

    }

    @Test
    public void SimpleTest() {
        System.out.println("simple test");

        String docString = getFileContents("linkTest.wmd");

        // search for a page-xml entity
        Matcher m = DocumentProcessor.getPageMatcher(docString);
        // if the record does not contain parsable page-xml
        if (!m.find()) return;

        WikiDocument doc = new WikiDocument();
        doc.setId(Integer.parseInt(m.group(3)));
        doc.setTitle(m.group(1));
        doc.setNS(Integer.parseInt(m.group(2)));
        doc.setText(StringUtils.unescapeEntities(m.group(4)));

        System.out.println(doc.getTitle());
        System.out.println(doc.getId());
        //System.out.println( doc.getText() );

        /*
        //List<Map.Entry<String, Integer>> links = doc.getOutLinks();
        Collector<Record> collector = new Collector<Record>() {
            @Override
            public void collect(Record record) {

                System.out.println(record.getField(0, LinkTupleOld.class).toString());
            }

            @Override
            public void close() {

            }
        };
        System.out.println(doc.getOutLinks());

        doc.collectLinks(collector);
         */
    }

    @Test
    public void RedirectTest() {

        Collector<WikiSimResult> collector = new Collector<WikiSimResult>() {
            @Override
            public void collect(WikiSimResult record) {

                System.out.println(record.toString());
            }

            @Override
            public void close() {

            }
        };

        String content = getFileContents("wikiRedirect.xml");
        new DocumentProcessor().flatMap(content, collector);

        System.out.println("#####");

        content = getFileContents("wikiSeeAlso.xml");
        new DocumentProcessor().flatMap(content, collector);

    }

    @Test
    public void RedirectTest2() {
        String content = getFileContents("wikiRedirect.xml");

        System.out.println(DocumentProcessor.getRedirectMatcher(content).find());

        Assert.that(DocumentProcessor.getRedirectMatcher(content).find(), "Redirect  not found");

    }

}