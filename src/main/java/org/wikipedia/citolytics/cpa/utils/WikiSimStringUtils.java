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
package org.wikipedia.citolytics.cpa.utils;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.commons.lang3.text.translate.EntityArrays;
import org.apache.commons.lang3.text.translate.LookupTranslator;

import java.util.ArrayList;
import java.util.List;

/**
 * @author rob
 */
public class WikiSimStringUtils {


    /**
     * Unescapes special entity char sequences like &lt; to its UTF-8 representation.
     * All ISO-8859-1, HTML4 and Basic entities will be translated.
     *
     * @param text the text that will be unescaped
     * @return the unescaped version of the string text
     */
    public static String unescapeEntities(String text) {
        CharSequenceTranslator iso = new LookupTranslator(EntityArrays.ISO8859_1_UNESCAPE());
        CharSequenceTranslator basic = new LookupTranslator(EntityArrays.BASIC_UNESCAPE());
        //CharSequenceTranslator html4 = new LookupTranslator(EntityArrays.HTML40_EXTENDED_UNESCAPE());
        return StringEscapeUtils.unescapeHtml4(iso.translate(basic.translate(text)));
    }

    public static String addCsvEnclosures(String value) {
        return "\"" + value.replace("\\", "\\\\") + "\"";
    }

    public static long hash(String string) {
        long h = 1125899906842597L; // prime
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = 31 * h + string.charAt(i);
        }
        return h;
    }

    public static ArrayList<Double> getDoubleListFromString(String delimitedString, String delimiterPattern) {
        ArrayList<Double> list = new ArrayList<>();
        String[] dbs = delimitedString.split(delimiterPattern);
        for (String db : dbs) {
            list.add(Double.valueOf(db));
        }
        return list;
    }

    public static boolean startsNotWith(String text, List<String> patterns) {
        for (String stopPattern : patterns) {
            if (text.startsWith(stopPattern)) {
                return false;
            }
        }
        return true;
    }

}
