package org.wikipedia.citolytics.multilang;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Represents mapping between two Wikipedia articles (based on article title)
 *
 * pageId of the referring page.
 * Language code of the target, in the ISO 639-1 standard.
 * Title of the target, including namespace (FULLPAGENAMEE style).
 *
 * e.g. from simplewiki dump >> (45907,'da','Tundra')
 * -> enwiki id = 30684
 * -> da id = 30027
 * -> simplewiki id = 45907
 *
 * @link https://www.mediawiki.org/wiki/Manual%3aLanglinks_table
 */
public class LangLinkTuple extends Tuple3<Integer, String, String> {
    public final static int PAGE_ID_KEY = 0;
    public final static int LANG_KEY = 1;
    public final static int TARGE_TITLE_KEY = 2;

    public LangLinkTuple() {
        // Flink requires empty constructor
    }

    public LangLinkTuple(int pageId, String lang, String targetTitle) {
        f0 = pageId;
        f1 = lang;
        f2 = targetTitle;
    }

    public int getPageId() {
        return f0;
    }

    public String getLang() {
        return f1;
    }

    public String getTargetTitle() {
        return f2;
    }
}
