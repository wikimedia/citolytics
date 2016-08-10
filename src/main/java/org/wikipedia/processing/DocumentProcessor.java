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
package org.wikipedia.processing;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.WikiDocument;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;
import org.wikipedia.citolytics.cpa.utils.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts links from Wikipedia documents, generates result records.
 */
public class DocumentProcessor extends RichFlatMapFunction<String, WikiSimResult> {

    public static String seeAlsoTitle = "==see also==";
    public static String seeAlsoRegex = "(^|\\W)" + seeAlsoTitle + "$";
    public static int seeAlsoRegexFlags = Pattern.MULTILINE + Pattern.CASE_INSENSITIVE;

    // WikiDump of 2006 does not contain namespace tags
    private boolean enableWiki2006 = false;

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);

        enableWiki2006 = parameter.getBoolean("wiki2006", true);
    }


    @Override
    public void flatMap(String content, Collector<WikiSimResult> out) {

        WikiDocument doc = processDoc(content);

        if (doc == null) return;

        doc.collectLinksAsResult(out);
    }

    public WikiDocument processDoc(String content) {
        return processDoc(content, false);
    }

    public WikiDocument processDoc(String content, boolean processSeeAlsoOnly) {
        // search for redirect -> skip if found
        if (getRedirectMatcher(content).find()) return null;

        // search for a page-xml entity
        Matcher m = getPageMatcher(content);
        // if the record does not contain parsable page-xml
        if (!m.find()) return null;

        // otherwise create a WikiDocument object from the xml
        WikiDocument doc = new WikiDocument();
        if (enableWiki2006) {
            doc.setId(Integer.parseInt(m.group(2)));
            doc.setTitle(StringUtils.unescapeEntities(m.group(1)));

            if (processSeeAlsoOnly) {
                doc.setText(getSeeAlsoSection(StringUtils.unescapeEntities(m.group(3))));
            } else {
                doc.setText(StringUtils.unescapeEntities(m.group(3)));
            }
        } else {
            // Default WikiXml
            doc.setId(Integer.parseInt(m.group(3)));
            doc.setTitle(StringUtils.unescapeEntities(m.group(1)));
            doc.setNS(Integer.parseInt(m.group(2)));

            // skip docs from namespaces other than
            if (doc.getNS() != 0) return null;

            if (processSeeAlsoOnly) {
                doc.setText(getSeeAlsoSection(StringUtils.unescapeEntities(m.group(4))));
            } else {
                doc.setText(StringUtils.unescapeEntities(m.group(4)));
            }
        }

        return doc;
    }


    public static Matcher getRedirectMatcher(String content) {
        Pattern redirect = Pattern.compile("<redirect", Pattern.CASE_INSENSITIVE);
        return redirect.matcher(content);
    }

    public Matcher getPageMatcher(String content) {

        // search for a page-xml entity
        // needle: title, (ns), id, text
        Pattern pageRegex;

        if (enableWiki2006) {
            pageRegex = Pattern.compile("(?:<page>\\s+)(?:<title>)(.*?)(?:</title>)\\s+(?:<id>)(.*?)(?:</id>)(?:.*?)(?:<text.*?>)(.*?)(?:</text>)", Pattern.DOTALL);
        } else {
            pageRegex = Pattern.compile("(?:<page>\\s+)(?:<title>)(.*?)(?:</title>)\\s+(?:<ns>)(.*?)(?:</ns>)\\s+(?:<id>)(.*?)(?:</id>)(?:.*?)(?:<text.*?>)(.*?)(?:</text>)", Pattern.DOTALL);
        }

        return pageRegex.matcher(content);
    }


    /**
     * get text of "See Also" section
     *
     * @param wikiText
     * @return seeAlsoText
     */
    public static String getSeeAlsoSection(String wikiText) {
        int seeAlsoStart = -1;
        String seeAlsoText = "";

        Pattern seeAlsoPattern = Pattern.compile(seeAlsoRegex, seeAlsoRegexFlags);
        Matcher seeAlsoMatcher = seeAlsoPattern.matcher(wikiText);

        if (seeAlsoMatcher.find()) {
            seeAlsoStart = seeAlsoMatcher.start();
        }

        if (seeAlsoStart > 0) {
            int seeAlsoEnd = seeAlsoStart + seeAlsoTitle.length();
            int nextHeadlineStart = wikiText.substring(seeAlsoStart + seeAlsoTitle.length()).indexOf("==");

            if (nextHeadlineStart > 0) {
                seeAlsoText = wikiText.substring(seeAlsoStart, seeAlsoEnd + nextHeadlineStart);
            } else {
                seeAlsoText = wikiText.substring(seeAlsoStart);
            }
        }

        return seeAlsoText;
    }
}








