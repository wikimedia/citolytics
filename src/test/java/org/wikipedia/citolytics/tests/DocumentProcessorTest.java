package org.wikipedia.citolytics.tests;

import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.tests.utils.Tester;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import static org.junit.Assert.assertEquals;

/**
 * @author malteschwarzer
 */
public class DocumentProcessorTest extends Tester {

    private void infoBoxRemoval(String filename) throws Exception {
        String wikiText = getFileContents(filename);
        DocumentProcessor dp = new DocumentProcessor();

        assertEquals("Infobox not removed (test indexOf) in " + filename, -1, dp.removeInfoBox(wikiText).indexOf(DocumentProcessor.INFOBOX_TAG));
    }
    @Test
    public void TestInfoBoxRemoval() throws Exception {
        infoBoxRemoval("DocumentProcessorTest/wikiInfoBox_1.xml");
    }

    @Test
    public void TestInfoBoxRemoval_MultipleInfoBoxes() throws Exception {
        infoBoxRemoval("DocumentProcessorTest/wikiInfoBox_2.xml");
    }

    @Test
    public void TestInfoBoxRemoval_NotClosingInfoBox() throws Exception {
        infoBoxRemoval("DocumentProcessorTest/wikiInfoBox_3.xml");
    }


    @Test
    public void testWordMapExtraction() throws Exception {
        String wikiText = getFileContents("DocumentProcessorTest/wikiInfoBox_1.xml");
        DocumentProcessor dp = new DocumentProcessor();

        assertEquals("Invalid word count returned", 26067, dp.processDoc(wikiText).getWordMap().size());

    }



    @Ignore
    @Test
    public void TestStaticCPI() throws Exception {
        String wikiText = getFileContents("DocumentProcessorTest/wikiInfoBox_1.xml");
        DocumentProcessor dp = new DocumentProcessor();

        WikiDocument doc = dp.processDoc(wikiText);

        // 1. sections by head lines (=, ==, ===)
        // 2. paragraphs (how to handle tables? via max distance?)
        // 3. sentences (...)

        // structure (linkname, section, paragraph, sentence)
        // -> map(linkname, distance_vector(level1, level2, ...))

        String text = dp.removeInfoBox(doc.getText());

//        System.out.println(text);

    }

}
