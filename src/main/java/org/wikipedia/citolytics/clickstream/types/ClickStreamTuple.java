package org.wikipedia.citolytics.clickstream.types;

import org.apache.flink.api.java.tuple.Tuple5;

import java.util.HashMap;

/**
 * Represents a item from the click stream data set:
 *
 *  (String) article name,
 *  (int) article id,
 *  (Integer) impressions,
 *  (HashMap<String, Integer>) clicks to link name / count
 *  HashMap<String, Integer> out ids
 */
public class ClickStreamTuple extends Tuple5<String, Integer, Integer, HashMap<String, Integer>, HashMap<String, Integer>> {
    public final static int ARTICLE_NAME_KEY = 0;
    public final static int ARTICLE_ID_KEY = 1;

    public ClickStreamTuple() {
        // Flink requires empty constructor
    }

    public ClickStreamTuple(String articleName, int articleId, int impressions, HashMap<String, Integer> outClicks, HashMap<String, Integer> outIds) {
        f0 = articleName;
        f1 = articleId;
        f2 = impressions;
        f3 = outClicks;
        f4 = outIds;
    }


    public String getArticleName() {
        return f0;
    }

    public int getArticleId() {
        return f1;
    }
    public int getImpressions() {
        return f2;
    }

    public HashMap<String, Integer> getOutClicks() {
        return f3;
    }


    public HashMap<String, Integer> getOutIds() {
        return f4;
    }
}
