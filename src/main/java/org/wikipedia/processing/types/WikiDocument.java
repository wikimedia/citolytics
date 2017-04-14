/*        __
 *        \ \
 *   _   _ \ \  ______
 *  | | | | > \(  __  )
 *  | |_| |/ ^ \| || |
 *  | ._,_/_/ \_\_||_|
 *  | |
 *  |_|
 * 
 * ----------------------------------------------------------------------------
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <rob ∂ CLABS dot CC> wrote this file. As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.
 * ----------------------------------------------------------------------------
 */
package org.wikipedia.processing.types;

import org.apache.commons.lang.StringUtils;
import org.wikipedia.processing.DocumentProcessor;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.abs;
import static java.lang.Math.max;

/**
 * Represents a Wikipedia documents including its text and properties, provides methods for link and word map extraction.
 */
public class WikiDocument {
    private DocumentProcessor processor;

    /**
     *   Used for extraction of links, e.g.
     *   [[Zielartikel|alternativer Text]]
     *   [[Artikelname]]
     *   [[#Wikilink|Wikilink]]
     */
    public final static Pattern LINKS_PATTERN = Pattern.compile("\\[\\[(.*?)((\\||#).*?)?\\]\\]");

    private java.util.List<java.util.Map.Entry<String, Integer>> outLinks = null;
    private TreeMap<Integer, Integer> wordMap = null;

    /**
     * Raw raw of the document
     */
    private String raw;

    /**
     * Title of the document
     */
    private String title;

    /**
     * Wikipedia id of the document
     */
    private int id;

    /**
     * Wikipedia pages belong to different namespaces. (e.g. 0: articles, ...)
     */
    private int ns;

    public WikiDocument() {
    }

    public WikiDocument(DocumentProcessor processor) {
        this.processor = processor;
    }

    private DocumentProcessor getDocumentProcessor() {
        if (this.processor == null) {
            this.processor = new DocumentProcessor();
        }
        return this.processor;
    }

    /**
     * Returns the document id.
     *
     * @return id of the document
     */
    public int getId() {
        return id;
    }

    /**
     * Sets the id of the document
     *
     * @param id
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Returns the document title.
     *
     * @return title of the document
     */
    public String getTitle() {
        return title;
    }

    /**
     * Sets the title of the document
     *
     * @param title
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Returns the namespace id of the document.
     *
     * @return namespace id
     */
    public int getNS() {
        return ns;
    }

    /**
     * Sets the namespace of the document.
     *
     * @param ns
     */
    public void setNS(int ns) {
        this.ns = ns;
    }

    /**
     * Returns the raw raw body of the document.
     *
     * @return the raw body
     */
    public String getText() {
        return raw;
    }

    /**
     * Sets the raw body of the document.
     */
    public void setText(String text) {
        this.raw = text;
    }

    public String getCleanText() {
        return getDocumentProcessor().cleanText(raw);
    }

    /**
     * Extract headlines from article content.
     * <p/>
     * Wiki-Markup:
     * <p/>
     * ==Section==
     * ===Subsection===
     * ====Sub-subsection===
     *
     * @return List of headlines
     */
    public List<String> getHeadlines() {
        List<String> headlines = new ArrayList<>();

        Pattern regex = Pattern.compile("^([=]{1,3})(.+)([=]{1,3})$", Pattern.MULTILINE);
        Matcher matcher = regex.matcher(raw);

        while (matcher.find()) {
            headlines.add(matcher.group(0).trim());
        }

        return headlines;
    }


    private void extractLinks() {
        // Clean text
        String text = getCleanText();

        // Reset outlinks
        outLinks = new ArrayList<>();
        Matcher m = LINKS_PATTERN.matcher(text);

        while (m.find()) {
            if (m.groupCount() >= 1) {
//                String target = m.group(1).trim();
//
//                if (target.length() > 0
//                        && !target.contains("<")
//                        && !target.contains(">")
//                        // Test for invalid namespaces -> check for ":"
//                        && !target.contains(":")
//                        ) {
//                    // First char is not case sensitive
//                    target = StringUtils.capitalize(target);
//                    outLinks.add(new AbstractMap.SimpleEntry<>(target, m.start()));
//                }
                String target = validateLinkTarget(m.group(1));
                if(target != null) {
                    outLinks.add(new AbstractMap.SimpleEntry<>(target, m.start()));
                }
            }
        }
    }

    public static String validateLinkTarget(String target) {
        target = target.trim();

        if (target.length() > 0
                && !target.contains("<")
                && !target.contains(">")
                // Test for invalid namespaces -> check for ":"
                && !target.contains(":")
                ) {
            // First char is not case sensitive
            target = StringUtils.capitalize(target);
            return target;
        } else {
            return null;
        }
    }

    private void SplitWS() {
        Pattern p = Pattern.compile("\\s+");
        String text = raw;
        Matcher m = p.matcher(text);
        int currentWS = 0;
        wordMap = new TreeMap<>();
        wordMap.put(0, 0);
        while (m.find()) {
            currentWS++;
            wordMap.put(m.start(), currentWS);
        }
    }

    public double getAvgLinkDistance() {
        getOutLinks();
        getWordMap();

        double linkPairs = 0;
        double distance = 0;

        // Loop all link pairs
        for (Map.Entry<String, Integer> outLink1 : outLinks) {
            for (Map.Entry<String, Integer> outLink2 : outLinks) {
                int order = outLink1.getKey().compareTo(outLink2.getKey());
                if (order > 0) {
                    int w1 = wordMap.floorEntry(outLink1.getValue()).getValue();
                    int w2 = wordMap.floorEntry(outLink2.getValue()).getValue();

                    distance += max(abs(w1 - w2), 1);
                    linkPairs++;
                }
            }
        }

        return distance / linkPairs;
    }

    public TreeMap<Integer, Integer> getWordMap() {
        if (wordMap == null) {
            SplitWS();
        }
        return wordMap;
    }

    public List<Map.Entry<String, Integer>> getOutLinks() {
        if (outLinks == null) {
            extractLinks();
        }
        return outLinks;
    }


}
