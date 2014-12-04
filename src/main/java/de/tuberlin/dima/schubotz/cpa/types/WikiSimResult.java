package de.tuberlin.dima.schubotz.cpa.types;

import org.apache.flink.api.java.tuple.Tuple9;

/**
 * Result for WikiSim
 * <p/>
 * 0 hash
 * 1 linkTuple
 * 2 distance
 * 3 count
 * 4 distSquared
 * 5 CPA
 * 6 minDist
 * 7 maxDist
 * 8 median
 */
public class WikiSimResult extends Tuple9<Long, LinkTuple, Long, Long, Long, Double, Long, Long, Double> {

    public WikiSimResult() {

    }

    public WikiSimResult(LinkTuple link, int distance, int count) {

        setField(link.getHash(), 0);
        setField(link, 1);

        setField(Long.valueOf(distance), 2);
        setField(Long.valueOf(count), 3); //  count

        setField(new Long(0), 4);
        //setField(new WikiSimBigDecimal(), 5);
        setField(new Double(0), 5);

        setField(new Long(0), 6);
        setField(new Long(0), 7);
        //setField(new ResultList(), 7);
        //setField(new WikiSimBigDecimal(), 8);
        setField(new Double(0), 8);

    }

    public void setDistance(long distance) {
        setField(distance, 2);
    }

    public void setCount(long count) {
        setField(count, 3);
    }

    public void setDistSquared(long distSquared) {
        setField(distSquared, 4);
    }

    public void setCPA(double cpa) {
        setField(cpa, 5);
    }

    public void setMin(long min) {
        setField(min, 6);
    }

    public void setMax(long max) {
        setField(max, 7);
    }

    public long getDistance() {
        return getField(2);
    }

    public long getCount() {
        return getField(3);
    }

    public long getDistSquared() {
        return getField(4);
    }

    /*public WikiSimBigDecimal getCPA() {
        return getField(5);
    }*/

    public double getCPA() {
        return getField(5);
    }

    public long getMin() {
        return getField(6);
    }

    public long getMax() {
        return getField(7);
    }
}
