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
package de.tuberlin.dima.schubotz.cpa.contracts;

import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.cpa.types.WikiSimResult;
import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DocumentProcessor implements FlatMapFunction<String, WikiSimResult> {
    @Override
    public void flatMap(String content, Collector<WikiSimResult> out) {

        WikiDocument doc = processDoc(content);

        if (doc == null) return;

        doc.collectLinksAsResult(out);
    }

    public static WikiDocument processDoc(String content) {
        return processDoc(content, false);
    }

    public static WikiDocument processDoc(String content, boolean processSeeAlsoOnly) {
        // search for redirect -> skip if found
        if (getRedirectMatcher(content).find()) return null;

        // search for a page-xml entity
        Matcher m = getPageMatcher(content);
        // if the record does not contain parsable page-xml
        if (!m.find()) return null;

        // otherwise create a WikiDocument object from the xml
        WikiDocument doc = new WikiDocument();
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

        return doc;
    }


    public static Matcher getRedirectMatcher(String content) {
        Pattern redirect = Pattern.compile("<redirect", Pattern.CASE_INSENSITIVE);
        return redirect.matcher(content);
    }

    public static Matcher getPageMatcher(String content) {

        // search for a page-xml entity
        Pattern pageRegex = Pattern.compile("(?:<page>\\s+)(?:<title>)(.*?)(?:</title>)\\s+(?:<ns>)(.*?)(?:</ns>)\\s+(?:<id>)(.*?)(?:</id>)(?:.*?)(?:<text.*?>)(.*?)(?:</text>)", Pattern.DOTALL);
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
        String seeAlsoTitle = "==see also==";
        Pattern seeAlsoPattern = Pattern.compile("^" + seeAlsoTitle + "$", Pattern.CASE_INSENSITIVE + Pattern.MULTILINE);
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








