package org.wikipedia.citolytics.tests;

import org.junit.Test;
import org.wikipedia.citolytics.cpa.types.WikiDocument;
import org.wikipedia.citolytics.tests.utils.Tester;
import org.wikipedia.processing.DocumentProcessor;

import static org.junit.Assert.assertEquals;

/**
 * namespaces
 * http://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces
 */

public class ParserTest extends Tester {

    @Test
    public void testLinkCount() {
        WikiDocument doc = new WikiDocument();
        doc.setText(getFileContents("wikiSeeAlso.xml"));

        // With infobox removal = 586
        // Without infobox removal = 618
        // with old namespace check = 586
        assertEquals("Link count wrong in wikiSeeAlso.xml", 585, doc.getOutLinks().size());

        doc = new WikiDocument();
        doc.setText(getFileContents("wikiTalkPage.xml"));
        // with old namespace check = 150
        assertEquals("Link count wrong in wikiTalkPage.xml", 147, doc.getOutLinks().size());

        doc = new WikiDocument();
        doc.setText(getFileContents("linkTest.wmd"));
        // with old namespace check = 36
        assertEquals("Link count wrong in linkTest.wmd", 36, doc.getOutLinks().size());

    }

    @Test
    public void nameSpaceTest() {
        assertEquals("TalkPage namespace is not skipped.", null, new DocumentProcessor().processDoc(getFileContents("wikiTalkPage.xml")));
    }

    @Test
    public void RedirectTest2() throws Exception {
        String content = getFileContents("wikiRedirect.xml");

        if (!DocumentProcessor.getRedirectMatcher(content).find())
            throw new Exception("Redirect  not found");

    }

}