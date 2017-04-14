package org.wikipedia.citolytics.cpa.types;

/**
 * Position of a link within a Wikipedia article. Position can be defined purely on number of words, but also based
 * on structural elements, e.g. headline and paragraph level.
 */

public class LinkPosition {
    public final static double CPI_SENTENCE_LEVEL = 1.0 / 2.0;
    public final static double CPI_PARAGRAPH_LEVEL = 1.0 / 4.0;
    public final static double CPI_SUBSECTION_LEVEL = 1.0 / 8.0;
    public final static double CPI_SECTION_LEVEL = 1.0 / 12.0;
    public final static double CPI_ARTICLE_LEVEL = 1.0 / 20.0;

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
        return "LinkPosition(hl2=" + hl2 + "; hl3=" + hl3 + "; paragraph=" + paragraph + "; words=" + words + ")";
    }

    public double getProximity(LinkPosition otherLink) {
        if (paragraph == otherLink.paragraph) {
            return CPI_PARAGRAPH_LEVEL; // Paragraph level
        } else if (hl3 == otherLink.hl3) {
            return CPI_SUBSECTION_LEVEL; // Sub-section level
        } else if (hl2 == otherLink.hl2) {
            return CPI_SECTION_LEVEL; // Section level
        } else {
            return CPI_ARTICLE_LEVEL; // Article level
        }
    }
}