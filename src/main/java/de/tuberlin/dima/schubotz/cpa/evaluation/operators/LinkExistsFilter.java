package de.tuberlin.dima.schubotz.cpa.evaluation.operators;

import de.tuberlin.dima.schubotz.cpa.evaluation.types.LinkResult;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.MLTResult;
import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Filter links which already exists in link graph
 * <p/>
 * first: results (T)
 * second: linkgraph (LinkResult)
 */
public class LinkExistsFilter<T extends Tuple> implements CoGroupFunction<T, LinkResult, T> {

    private int link1key;
    private int link2key;

    public LinkExistsFilter(int link1, int link2) {
        link1key = link1;
        link2key = link2;
    }

    @Override
    public void coGroup(Iterable<T> first, Iterable<LinkResult> second, Collector<T> out) throws Exception {
        // if not exists?
        Iterator<T> firstIterator = first.iterator();
        Iterator<LinkResult> secondIterator = second.iterator();

        if (firstIterator.hasNext()) {
            T record = firstIterator.next();

            long hashA = StringUtils.hash((String) record.getField(link1key) + (String) record.getField(link2key));
            long hashB = StringUtils.hash((String) record.getField(link2key) + (String) record.getField(link1key));

            while (secondIterator.hasNext()) {
                LinkResult link = secondIterator.next();

                long hash = StringUtils.hash((String) link.getField(0) + (String) link.getField(1));

                if (hash == hashA || hash == hashB)
                    return;
            }

            out.collect(record);
        }

    }
}
