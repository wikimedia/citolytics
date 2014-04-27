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
import eu.stratosphere.types.*;
import eu.stratosphere.util.Collector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.abs;
import static java.lang.Math.pow;

/**
 * @author rob
 */
public class WikiDocument implements Value {
    private final LinkTuple linkTuple = new LinkTuple();
    private final StringValue LeftLink = new StringValue();
    private final StringValue RightLink = new StringValue();
    /**
     * reciprocal distance *
     */
    private final DoubleValue recDistance = new DoubleValue();
    private final IntValue count = new IntValue(1);
    private final Record target = new Record();
    private java.util.List<java.util.Map.Entry<String, Integer>> outLinks = null;
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
     *
    public String getPlainText() {
        StringWriter writer = new StringWriter();
        MarkupParser parser = new MarkupParser();
        MarkupLanguage wiki = new MediaWikiLanguage();
        parser.setMarkupLanguage(wiki);
        parser.setBuilder(new PlaintextDocumentBuilder(writer));
        parser.parse(raw.getValue());
        plaintext.setValue(writer.toString());
        return plaintext.getValue();
    } */

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        ns.write(out);
        title.write(out);
        raw.write(out);
        //plaintext.write(out);
    }

    @Override
    public void read(DataInput in) throws IOException {
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

    private void extractLinks() {
        Pattern p = Pattern.compile("\\[\\[(.*?)(\\|.*?)?\\]\\]");
        String text = raw.getValue();
        Matcher m = p.matcher(text);

        outLinks = new ArrayList<>();
        while (m.find()) {
            if (m.groupCount() >= 1) {
                outLinks.add(new AbstractMap.SimpleEntry<>(m.group(1).trim(), m.start()));
            }
        }
    }

    public void collectLinks(Collector<Record> collector) {
        getOutLinks();
        for (Map.Entry<String, Integer> outLink : outLinks) {
            for (Map.Entry<String, Integer> outLink2 : outLinks) {
                int order = outLink.getKey().compareTo(outLink2.getKey());
                if (order != 0) {
                    //TODO: Adjust distance metrics not to use chars but words or whatever is suitable here
                    recDistance.setValue(1 / (pow(abs(outLink2.getValue() - outLink.getValue()), 1)));
                    LeftLink.setValue(outLink.getKey());
                    RightLink.setValue(outLink2.getKey());
                    if (order < 0) {
                        linkTuple.setFirst(LeftLink);
                        linkTuple.setSecond(RightLink);
                    } else {
                        linkTuple.setSecond(LeftLink);
                        linkTuple.setFirst(RightLink);
                    }
                    target.clear();
                    target.addField(linkTuple);
                    target.addField(recDistance);
                    target.addField(count);
                    collector.collect(target);
                }
            }

        }
    }

    public List<Map.Entry<String, Integer>> getOutLinks() {
        if (outLinks == null) {
            extractLinks();
        }
        return outLinks;
    }
}
