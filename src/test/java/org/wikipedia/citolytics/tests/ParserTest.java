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

        assertEquals("Link count wrong in wikiSeeAlso.xml", 618, doc.getOutLinks().size());

        doc = new WikiDocument();
        doc.setText(getFileContents("wikiTalkPage.xml"));

        assertEquals("Link count wrong in wikiTalkPage.xml", 150, doc.getOutLinks().size());

        doc = new WikiDocument();
        doc.setText(getFileContents("linkTest.wmd"));

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