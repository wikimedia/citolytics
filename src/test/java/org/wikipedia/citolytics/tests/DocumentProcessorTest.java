package org.wikipedia.citolytics.tests;

import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.cpa.utils.WikiSimStringUtils;
import org.wikipedia.citolytics.tests.utils.Tester;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * @author malteschwarzer
 */
public class DocumentProcessorTest extends Tester {
    @Test
    public void testGetInvalidNameSpaces() throws Exception {
        DocumentProcessor dp = new DocumentProcessor();

        assertEquals("Invalid count of invalid namespaces (re-run py script?)", 320, dp.getInvalidNameSpaces().size());
    }

    private void infoBoxRemoval(String filename) throws Exception {
        String wikiText = getFileContents(filename);
        DocumentProcessor dp = new DocumentProcessor();

        assertEquals("Infobox not removed (test indexOf) in " + filename, -1, dp.removeInfoBox(wikiText).indexOf(DocumentProcessor.INFOBOX_TAG));
    }
    @Test
    public void TestInfoBoxRemoval() throws Exception {
        infoBoxRemoval("wikiInfoBox_1.xml");
    }

    @Test
    public void TestInfoBoxRemoval_MultipleInfoBoxes() throws Exception {
        infoBoxRemoval("wikiInfoBox_2.xml");
    }

    @Test
    public void TestInfoBoxRemoval_NotClosingInfoBox() throws Exception {
        infoBoxRemoval("wikiInfoBox_3.xml");
    }

    private Pattern getHeadlinePattern(int level) {
//        return "([\\w\\s]+)([=]{" + level + "})";
        return Pattern.compile("([=]{" + level + "})([\\w\\s]+)([=]{" + level + "})$", Pattern.MULTILINE);

    }

    @Ignore
    @Test
    public void TestStaticCPI() throws Exception {
        String wikiText = getFileContents("wikiInfoBox_1.xml");
        DocumentProcessor dp = new DocumentProcessor();

        WikiDocument doc = dp.processDoc(wikiText);

        // 1. sections by head lines (=, ==, ===)
        // 2. paragraphs (how to handle tables? via max distance?)
        // 3. sentences (...)

        // structure (linkname, section, paragraph, sentence)
        // -> map(linkname, distance_vector(level1, level2, ...))

        String text = dp.removeInfoBox(doc.getText());

//        System.out.println(text);

        Pattern hl2_pattern = getHeadlinePattern(2);
        Pattern hl3_pattern = getHeadlinePattern(3);
        Pattern paragraph_pattern = Pattern.compile("^$", Pattern.MULTILINE);

        int hl2_counter = 0, hl3_counter = 0, paragraph_counter = 0;

        Map<String, LinkPosition> links = new HashMap<>();
        String linkTarget;

        String[] hl2s = hl2_pattern.split(text);
        for (String hl2 : hl2s) {
            String[] hl3s = hl3_pattern.split(hl2);

            for (String hl3 : hl3s) {
                String[] paragraphs = paragraph_pattern.split(hl3);

                for (String paragraph : paragraphs) {

                    Pattern p = Pattern.compile("\\[\\[(.*?)((\\||#).*?)?\\]\\]");
                    Matcher m = p.matcher(paragraph);

                    while (m.find()) {
                        if (m.groupCount() >= 1) {
                            linkTarget = m.group(1).trim();

                            if (linkTarget.length() > 0
                                    && !linkTarget.contains("<")
                                    && !linkTarget.contains(">")
                                    && WikiSimStringUtils.startsNotWith(linkTarget.toLowerCase(), dp.getInvalidNameSpaces())) {
                                // First char is not case sensitive
                                linkTarget = StringUtils.capitalize(linkTarget);
                                links.put(linkTarget, new LinkPosition(hl2_counter, hl3_counter, paragraph_counter, m.start()));
                            }
                        }
                    }

                    System.out.println(paragraph + "\n\n### </paragraph> ======");
                    paragraph_counter++;
                }

                System.out.println("### </hl3>");
                hl3_counter++;
            }
            hl2_counter++;
            System.out.println("### </hl2>");
        }

        System.out.println(links);
    }

    class LinkPosition {
        public int hl2;
        public int hl3;
        public int paragraph;
        public int words;

        public LinkPosition(int hl2, int hl3, int paragraph, int words) {
            this.hl2 = hl2;
            this.hl3 = hl3;
            this.paragraph = paragraph;
            this.words = words;
        }

        public String toString() {
            return "DistanceVector(hl2=" + hl2 + "; hl3=" + hl3 + "; paragraph=" + paragraph + "; words=" + words + ")";
        }

        public double getProximity(LinkPosition otherLink) {
            if (paragraph == otherLink.paragraph) {
                return 1.0 / 2.0; // Paragraph level
            } else if (hl3 == otherLink.hl3) {
                return 1.0 / 4.0; // Sub-section level
            } else if (hl2 == otherLink.hl2) {
                return 1.0 / 8.0; // Section level
            } else {
                return 1.0 / 10.0; // Article level
            }
        }
    }
}
