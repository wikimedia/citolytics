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

import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.abs;
import static java.lang.Math.max;

/**
 * @author rob
 */
public class WikiDocument implements Value {

    // namespaces from http://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces
    private final ArrayList<String> listOfStopPatterns = new ArrayList<String>(Arrays.asList(
            "media:", "special:", "talk:", "user:", "user talk:", "wikipedia:", "wikipedia talk:", "file:", "file talk:", "mediawiki:", "mediawiki talk:",
            "template:", "template talk:", "help:", "help talk:", "category:", "category talk:", "portal:", "portal talk:", "book:", "book talk:",
            "draft:", "draft talk:", "education program:", "education program talk:", "timedtext:", "timedtext talk:", "module:", "module talk:", "topic:",
            "image:"
    ));

    private final LinkTuple linkTuple = new LinkTuple();
    private final StringValue LeftLink = new StringValue();
    private final StringValue RightLink = new StringValue();
    /**
     * reciprocal distance *
     */
    //private final DoubleValue recDistance = new DoubleValue();
    private final IntValue distance = new IntValue(1);
    private final IntValue count = new IntValue(1);
    private final Record target = new Record();
    private java.util.List<java.util.Map.Entry<String, Integer>> outLinks = null;
    private TreeMap<Integer, Integer> wordMap = null;
    /*
     * Raw raw of the document
     */
    private StringValue raw = new StringValue();

    /*
     * Plaintext version of the document
     *
    private StringValue plaintext = new StringValue();

    /*
     * Title of the document
     */
    private StringValue title = new StringValue();

    /*
     * Wikipedia id of the document
     */
    private IntValue id = new IntValue();

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
    private IntValue ns = new IntValue();


    /**
     * Returns a plaintext version of this document.
     *
     * @return a plaintext string
     * <p/>
     * public String getPlainText() {
     * StringWriter writer = new StringWriter();
     * MarkupParser parser = new MarkupParser();
     * MarkupLanguage wiki = new MediaWikiLanguage();
     * parser.setMarkupLanguage(wiki);
     * parser.setBuilder(new PlaintextDocumentBuilder(writer));
     * parser.parse(raw.getValue());
     * plaintext.setValue(writer.toString());
     * return plaintext.getValue();
     * }
     */

    @Override
    public void write(DataOutputView out) throws IOException {
        id.write(out);
        ns.write(out);
        title.write(out);
        raw.write(out);
        //plaintext.write(out);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        id.read(in);
        ns.read(in);
        title.read(in);
        raw.read(in);
        //plaintext.read(in);
    }

    /**
     * Returns the document id.
     *
     * @return id of the document
     */
    public int getId() {
        return id.getValue();
    }

    /**
     * Sets the id of the document
     *
     * @param id
     */
    public void setId(Integer id) {
        this.id.setValue(id);
    }

    /**
     * Returns the document title.
     *
     * @return title of the document
     */
    public String getTitle() {
        return title.getValue();
    }

    /**
     * Sets the title of the document
     *
     * @param title
     */
    public void setTitle(String title) {
        this.title.setValue(title);
    }

    /**
     * Returns the namespace id of the document.
     *
     * @return namespace id
     */
    public int getNS() {
        return ns.getValue();
    }

    /**
     * Sets the namespace of the document.
     *
     * @param ns
     */
    public void setNS(int ns) {
        this.ns.setValue(ns);
    }

    /**
     * Returns the raw raw body of the document.
     *
     * @return the raw body
     */
    public String getText() {
        return raw.getValue();
    }

    /**
     * Sets the raw body of the document.
     */
    public void setText(String text) {
        this.raw.setValue(StringUtils.unescapeEntities(text));
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
     * removes "See Also" section from wiki page xml
     *
     * @param wikiXmlLowerCase
     * @return wikiXml with out "See Also" section
     */
    public String extractSeeAlsoSection(String wikiXmlLowerCase) {
        String seeAlsoText = "";
        String pattern = "==see also==";
        int seeAlsoStart = wikiXmlLowerCase.indexOf(pattern);

        if (seeAlsoStart > 0) {
            int nextHeadlineStart = wikiXmlLowerCase.substring(seeAlsoStart + pattern.length()).indexOf("==");

            if (nextHeadlineStart > 0) {
                seeAlsoText = wikiXmlLowerCase.substring(seeAlsoStart, seeAlsoStart + pattern.length() + nextHeadlineStart);

                wikiXmlLowerCase = wikiXmlLowerCase.substring(0, seeAlsoStart);
                wikiXmlLowerCase += seeAlsoText.replaceAll("\\[\\[(.*?)((\\||#).*?)?\\]\\]", "[[SEEALSO_$1]]");
                wikiXmlLowerCase += wikiXmlLowerCase.substring(seeAlsoStart + pattern.length() + nextHeadlineStart);
            } else {
                seeAlsoText = wikiXmlLowerCase.substring(seeAlsoStart);
                wikiXmlLowerCase = wikiXmlLowerCase.substring(0, seeAlsoStart);
                wikiXmlLowerCase += seeAlsoText.replaceAll("\\[\\[(.*?)((\\||#).*?)?\\]\\]", "[[SEEALSO_$1]]");
            }
        }

        return wikiXmlLowerCase;
    }

    private void extractLinks() {
        outLinks = new ArrayList<>();

        // [[Zielartikel|alternativer Text]]
        // [[Artikelname]]
        // [[#Wikilink|Wikilink]]
        Pattern p = Pattern.compile("\\[\\[(.*?)((\\||#).*?)?\\]\\]");

        // lowercase
        String text = raw.getValue().toLowerCase();

        // strip "see also" section
        text = extractSeeAlsoSection(text);

        /* Remove all interwiki links */
        Pattern p2 = Pattern.compile("\\[\\[(\\w\\w\\w?|simple)(-[\\w-]*)?:(.*?)\\]\\]");
        text = p2.matcher(text).replaceAll("");
        Matcher m = p.matcher(text);

        while (m.find()) {
            if (m.groupCount() >= 1) {
                String target = m.group(1).trim();

                if (target.length() > 0
                        && !target.contains("<")
                        && !target.contains(">")
                        && startsNotWith(target, listOfStopPatterns)) {
                    outLinks.add(new AbstractMap.SimpleEntry<>(target, m.start()));
                }
            }
        }
    }

    private void SplitWS() {
        Pattern p = Pattern.compile("\\s+");
        String text = raw.getValue();
        Matcher m = p.matcher(text);
        int currentWS = 0;
        wordMap = new TreeMap<>();
        wordMap.put(0, 0);
        while (m.find()) {
            currentWS++;
            wordMap.put(m.start(), currentWS);
        }
    }

    public void collectLinks(Collector<Record> collector) {
        //Skip all namespaces other than main
        if (ns.getValue() != 0) {
            return;
        }
        getOutLinks();
        getWordMap();
        for (Map.Entry<String, Integer> outLink1 : outLinks) {
            for (Map.Entry<String, Integer> outLink2 : outLinks) {
                int order = outLink1.getKey().compareTo(outLink2.getKey());
                if (order > 0) {
                    int w1 = wordMap.floorEntry(outLink1.getValue()).getValue();
                    int w2 = wordMap.floorEntry(outLink2.getValue()).getValue();
                    int d = max(abs(w1 - w2), 1);
                    distance.setValue(d);
                    //recDistance.setValue(1 / (pow(d, α)));
                    LeftLink.setValue(outLink1.getKey());
                    RightLink.setValue(outLink2.getKey());
                    linkTuple.setFirst(LeftLink);
                    linkTuple.setSecond(RightLink);

                    target.clear();
                    target.addField(linkTuple);
                    target.addField(distance);
                    target.addField(count);
                    collector.collect(target);
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
