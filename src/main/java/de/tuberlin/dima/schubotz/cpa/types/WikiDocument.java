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
package de.tuberlin.dima.schubotz.cpa.types;

import org.apache.flink.util.Collector;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.abs;
import static java.lang.Math.max;

/**
 * @author rob
 */
public class WikiDocument {

    // namespaces from http://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces
    private final ArrayList<String> listOfStopPatterns = new ArrayList<String>(Arrays.asList(
            "media:", "special:", "talk:", "user:", "user talk:", "wikipedia:", "wikipedia talk:", "file:", "file talk:", "mediawiki:", "mediawiki talk:",
            "template:", "template talk:", "help:", "help talk:", "category:", "category talk:", "portal:", "portal talk:", "book:", "book talk:",
            "draft:", "draft talk:", "education program:", "education program talk:", "timedtext:", "timedtext talk:", "module:", "module talk:", "topic:",
            "image:"
    ));

    private final LinkTuple linkTuple = new LinkTuple();

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

    private boolean startsNotWith(String text, ArrayList<String> patterns) {
        for (String stopPattern : patterns) {
            if (text.startsWith(stopPattern)) {
                return false;
            }
        }
        return true;
    }

    /**
     * remove links of "See Also" section
     *
     * @param wikiText
     * @return wikiText without "See Also" links
     */
    public String stripSeeAlsoSection(String wikiText) {
        int seeAlsoStart = -1;
        String seeAlsoTitle = "==see also==";
        Pattern seeAlsoPattern = Pattern.compile("^" + seeAlsoTitle, Pattern.CASE_INSENSITIVE + Pattern.MULTILINE);
        Matcher seeAlsoMatcher = seeAlsoPattern.matcher(wikiText);

        if (seeAlsoMatcher.find()) {
            seeAlsoStart = seeAlsoMatcher.start();
        }

        // See also section exists
        if (seeAlsoStart > 0) {
            int seeAlsoEnd = seeAlsoStart + seeAlsoTitle.length();
            int nextHeadlineStart = wikiText.substring(seeAlsoStart + seeAlsoTitle.length()).indexOf("==");

            String strippedWikiText = wikiText.substring(0, seeAlsoStart);

            // Append content after see also section
            if (nextHeadlineStart > 0) {
                strippedWikiText += wikiText.substring(nextHeadlineStart + seeAlsoEnd);
            }

            return strippedWikiText;
        }

        return wikiText;
    }

    private void extractLinks() {
        outLinks = new ArrayList<>();

        // [[Zielartikel|alternativer Text]]
        // [[Artikelname]]
        // [[#Wikilink|Wikilink]]
        Pattern p = Pattern.compile("\\[\\[(.*?)((\\||#).*?)?\\]\\]");

        String text = raw; //.toLowerCase();

        // strip "see also" section
        text = stripSeeAlsoSection(text);

        /* Remove all interwiki links */
        Pattern p2 = Pattern.compile("\\[\\[(\\w\\w\\w?|simple)(-[\\w-]*)?:(.*?)\\]\\]");
        text = p2.matcher(text).replaceAll("");
        Matcher m = p.matcher(text);

        while (m.find()) {
            if (m.groupCount() >= 1) {
                target = m.group(1).trim();

                if (target.length() > 0
                        && !target.contains("<")
                        && !target.contains(">")
                        && startsNotWith(target.toLowerCase(), listOfStopPatterns)) {
                    // First char is not case sensitive
                    target = org.apache.commons.lang.StringUtils.capitalize(target);
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

    public void collectLinksAsResult(Collector<WikiSimResult> collector) {
        //Skip all namespaces other than main
        if (ns != 0) {
            return;
        }
        getOutLinks();
        getWordMap();

        // Loop all link pairs
        for (Map.Entry<String, Integer> outLink1 : outLinks) {
            for (Map.Entry<String, Integer> outLink2 : outLinks) {
                int order = outLink1.getKey().compareTo(outLink2.getKey());
                if (order > 0) {
                    int w1 = wordMap.floorEntry(outLink1.getValue()).getValue();
                    int w2 = wordMap.floorEntry(outLink2.getValue()).getValue();
                    int d = max(abs(w1 - w2), 1);
                    //recDistance.setValue(1 / (pow(d, α)));

                    linkTuple.setFirst(outLink1.getKey());
                    linkTuple.setSecond(outLink2.getKey());

                    // Add result to collector
                    if (linkTuple.isValid()) {
                        collector.collect(new WikiSimResult(linkTuple, d));
                    }
                }
            }
        }
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
