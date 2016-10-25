package org.wikipedia.citolytics.tests;

import org.junit.Test;
import org.wikipedia.citolytics.cpa.types.WikiDocument;
import org.wikipedia.citolytics.tests.utils.Tester;
import org.wikipedia.processing.DocumentProcessor;

import static org.junit.Assert.assertEquals;

/**
 * @author malteschwarzer
 */
public class DocumentProcessorTest extends Tester {

    @Test
    public void TestInfoBoxRemoval() throws Exception {
        String wikiText = getFileContents("wikiInfoBox.xml");
        DocumentProcessor dp = new DocumentProcessor();

        assertEquals("Infobox not removed (test indexOf)", -1, dp.removeInfoBox(wikiText).indexOf(DocumentProcessor.INFOBOX_TAG));

    }

    @Test
    public void TestStaticCPI() throws Exception {
        String wikiText = getFileContents("wikiInfoBox.xml");
        DocumentProcessor dp = new DocumentProcessor();

        WikiDocument doc = dp.processDoc(wikiText);

        // 1. sections by head lines (=, ==, ===)
        // 2. paragraphs (how to handle tables? via max distance?)
        // 3. sentences (...)

    }
}
