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
import org.wikipedia.citolytics.cpa.types.LinkPair;
import org.wikipedia.citolytics.cpa.utils.WikiSimStringUtils;
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

    private final LinkPair linkPair = new LinkPair();

    /**
     * reciprocal distance *
     */
    //private final DoubleValue recDistance = new DoubleValue();
    private String target = "";

    private java.util.List<java.util.Map.Entry<String, Integer>> outLinks = null;
    private TreeMap<Integer, Integer> wordMap = null;
    /*
     * Raw raw of the document
     */
    private String raw;

    /*
     * Plaintext version of the document
     *
    private StringValue plaintext = new StringValue();

    /*
     * Title of the document
     */
    private String title;

    /*
     * Wikipedia id of the document
     */
    private int id;

    /**
     * Wikipedia pages belong to different namespaces. Below
     * is a list that describes a commonly used namespaces.
     * <p/>
     * -2	Media
     * -1	Special
     * 0	Default
     * 1	Talk
     * 2	User
     * 3	User talk
     * 4	Wikipedia
     * 5	Wikipedia talk
     * 6	File
     * 7	File talk
     * 8	MediaWiki
     * 9	MediaWiki talk
     * 10	Template
     * 11	Template talk
     * 12	Help
     * 13	Help talk
     * 14	Category
     * 15	Category talk
     * 100	Portal
     * 101	Portal talk
     * 108	Book
     * 109	Book talk
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
        outLinks = new ArrayList<>();

        String text = getDocumentProcessor().cleanText(raw);

        // Search for links, e.g.
        // [[Zielartikel|alternativer Text]]
        // [[Artikelname]]
        // [[#Wikilink|Wikilink]]
        Pattern p = Pattern.compile("\\[\\[(.*?)((\\||#).*?)?\\]\\]");
        Matcher m = p.matcher(text);

        while (m.find()) {
            if (m.groupCount() >= 1) {
                target = m.group(1).trim();

                if (target.length() > 0
                        && !target.contains("<")
                        && !target.contains(">")
                        && WikiSimStringUtils.startsNotWith(target.toLowerCase(), getDocumentProcessor().getInvalidNameSpaces())
                        // Alternative test for invalid namespaces -> check for ":"
                        && !target.contains(":")
                        ) {
                    // First char is not case sensitive
                    target = StringUtils.capitalize(target);
                    outLinks.add(new AbstractMap.SimpleEntry<>(target, m.start()));
                }
            }
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
