package de.tuberlin.dima.schubotz.cpa.types;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;

/**
 * Result for WikiSim
 * <p/>
 * 0 Long   hash
 * 1 JPO    linkTuple
 * 2 Long   distance
 * 3 Integer   count
 * 4 Long   distSquared #
 * 5 Double CPA
 * 6 Long   minDist #
 * 7 Long   maxDist #
 * 8 Double median  #
 */
public class WikiSimResult extends Tuple6<Long, LinkTuple, Long, Integer, Long, Double> {
    private final boolean MINMAX = false;

    private final static int MAX_KEY = 7;
    private final static int MIN_KEY = 6;
    private final static int CPA_KEY = 5;
    private final static int DISTSQUARED_KEY = 4;
    private final static int COUNT_KEY = 3;
    private final static int DISTANCE_KEY = 2;

    public WikiSimResult() {

    }

    public WikiSimResult(LinkTuple link, int distance, int count) {

        setField(link.getHash(), 0);
        setField(link, 1);

        setDistance(distance);
        setCount(count);

        setDistSquared(0);
        setCPA(.0);
        setMin(0);
        setMax(0);

        setMedian(.0);

    }

    public void setDistance(long distance) {
        setField(distance, DISTANCE_KEY);
    }

    public void setDistance(int distance) {
        setField(Long.valueOf(distance), DISTANCE_KEY);
    }


    public void setCount(int count) {
        setField(count, COUNT_KEY);
    }

    public void setDistSquared(long distSquared) {
        setField(distSquared, DISTSQUARED_KEY);
    }

    public void setCPA(double cpa) {
        setField(cpa, CPA_KEY);
    }

    public void setMin(long min) {
        if (MINMAX)
            setField(min, MIN_KEY);
    }

    public void setMax(long max) {
        if (MINMAX)
            setField(max, MAX_KEY);
    }

    public void setMedian(double median) {
//        setField(median, 8);
    }

    public long getDistance() {
        return getField(DISTANCE_KEY);
    }

    public int getCount() {
        return getField(COUNT_KEY);
    }

    public long getDistSquared() {
        return getField(DISTSQUARED_KEY);
    }

    /*public WikiSimBigDecimal getCPA() {
        return getField(5);
    }*/

    public double getCPA() {
        return getField(CPA_KEY);
    }

    public long getMin() {
        if (MINMAX)
            return getField(MIN_KEY);
        else
            return 0;
    }

    public long getMax() {
        if (MINMAX)
            return getField(MAX_KEY);
        else
            return 0;
    }
}
