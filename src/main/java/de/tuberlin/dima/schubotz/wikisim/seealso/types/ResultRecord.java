package de.tuberlin.dima.schubotz.wikisim.seealso.types;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * Contains WikiSim results
 * <p/>
 * 0: Article
 * 1: List<WikiSimResults>
 */
public class ResultRecord<SORT extends Comparable> extends Tuple2<String, ArrayList<WikiSimComparableResult<SORT>>> {
    public ResultRecord() {

    }

    public ResultRecord(String articleName, ArrayList<WikiSimComparableResult<SORT>> results) {
        setField(articleName, 0);
        setField(results, 1);
    }

    public ResultRecord(String article, String result, SORT score) {
        ArrayList<WikiSimComparableResult<SORT>> list = new ArrayList<WikiSimComparableResult<SORT>>();

        list.add(new WikiSimComparableResult<SORT>(result, score));

        setField(article, 0);
        setField(list, 1);
    }
}
