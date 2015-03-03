package de.tuberlin.dima.schubotz.cpa.evaluation.operators;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.ListResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.ResultRecord;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.WikiSimComparableResult;
import de.tuberlin.dima.schubotz.cpa.types.list.StringListValue;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable;
import org.apache.flink.shaded.com.google.common.collect.Ordering;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Transform result set into EvaluationResult format:
 * <p/>
 * article name | array( result1, result2, ... ) | number of results
 *
 * @param <SORT> input format (Tuple)
 */

@Combinable
public class ListBuilder<SORT extends Comparable> extends RichGroupReduceFunction<ResultRecord<SORT>, ListResult> {
    int maxListLength;

    public ListBuilder(int maxListLength) {
        this.maxListLength = maxListLength;
    }


    @Override
    public void combine(Iterable<ResultRecord<SORT>> in, Collector<ResultRecord<SORT>> out) throws Exception {
        Iterator<ResultRecord<SORT>> iterator = in.iterator();
        ResultRecord<SORT> record = null;
        String articleName = null;

        // Convert to ComparableResult, add to unsortedList with fixed length
//        MinMaxPriorityQueue<WikiSimComparableResult<SORT>> unsortedQueue = MinMaxPriorityQueue.maximumSize(maxListLength).create();
        ArrayList<WikiSimComparableResult<SORT>> unsortedList = new ArrayList<>();

        while (iterator.hasNext()) {
            record = iterator.next();
            articleName = record.getField(0);

            unsortedList.addAll((List<WikiSimComparableResult<SORT>>) record.getField(1));
//            unsortedQueue.addAll((List<WikiSimComparableResult<SORT>>) record.getField(1));
        }

//        List<WikiSimComparableResult<SORT>> sortedList = Ordering.natural().greatestOf(unsortedList, maxListLength);


        if (articleName != null) {
            out.collect(new ResultRecord<>(articleName, new ArrayList<>(unsortedList)));
//            out.collect(new ResultRecord<>(articleName, new ArrayList<>(unsortedQueue)));
        } else {
            throw new Exception("Articlname not set!");
        }
    }

    @Override
    public void reduce(Iterable<ResultRecord<SORT>> in, Collector<ListResult> out) throws Exception {
        Iterator<ResultRecord<SORT>> iterator = in.iterator();
        ResultRecord<SORT> record = null;

        // Convert to ComparableResult, add to unsortedList with fixed length
//        MinMaxPriorityQueue<WikiSimComparableResult<SORT>> unsortedQueue = MinMaxPriorityQueue.maximumSize(maxListLength).create();
        List<WikiSimComparableResult<SORT>> unsortedList = new ArrayList<>();

        while (iterator.hasNext()) {
            record = iterator.next();
            unsortedList.addAll((List<WikiSimComparableResult<SORT>>) record.getField(1));

//            unsortedList.add(new WikiSimComparableResult<SORT>((String) record.getField(0), (String) record.getField(1), (SORT) record.getField(2)));
        }

        List<WikiSimComparableResult<SORT>> sortedList = Ordering.natural().greatestOf(unsortedList, maxListLength);
        // Add to ResultList
        StringListValue resultList = new StringListValue();
        for (WikiSimComparableResult<SORT> item : sortedList) {
            resultList.add(new StringValue(item.getSortField2()));
        }

        out.collect(new ListResult((String) record.getField(0), resultList, resultList.size()));
    }

}
