package org.wikipedia.citolytics.clickstream.types;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Represents a item from the click stream data set:
 *
 *  (String) article name,
 *  (int) article id,
 *  (Integer) impressions,
 *  (HashMap<String, Integer>) clicks to link name / count
 *  HashMap<String, Integer> out ids
 */
public class ClickStreamTranslateTuple extends Tuple6<String, Integer, Integer, String, Integer, Integer> {
    public final static int ARTICLE_NAME_KEY = 0;
    public final static int ARTICLE_ID_KEY = 1;
    public final static int ARTICLE_IMPRESSIONS = 2;
    public final static int TARGET_NAME_KEY = 3;
    public final static int TARGET_ID_KEY = 4;
    public final static int TARGET_CLICKS_KEY = 5;

    public ClickStreamTranslateTuple() {
        // Flink requires empty constructor
    }

    public ClickStreamTranslateTuple(String articleName, int articleId, int impressions, String targetName, int targetId, int clicks) {
        f0 = articleName;
        f1 = articleId;
        f2 = impressions;
        f3 = targetName;
        f4 = targetId;
        f5 = clicks;
    }

    public boolean hasIds() {
        return f1 != 0 && f4 != 0;
    }

    public String getArticleName() {
        return getField(ARTICLE_NAME_KEY);
    }

    public int getArticleId() {
        return getField(ARTICLE_ID_KEY);
    }

    public int getImpressions() {
        return getField(ARTICLE_IMPRESSIONS);
    }

    public String getTargetName() {
        return getField(TARGET_NAME_KEY);
    }

    public int getTargetId() {
        return getField(TARGET_ID_KEY);
    }

    public int getClicks() {
        return getField(TARGET_CLICKS_KEY);
    }

    public void setArticleId(int id) {
        setField(id, ARTICLE_ID_KEY);
    }

    public void setTargetId(int id) {
        setField(id, TARGET_ID_KEY);
    }

}
