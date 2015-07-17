package de.tuberlin.dima.schubotz.cpa.types;

import de.tuberlin.dima.schubotz.cpa.types.list.DoubleListValue;
import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Result for WikiSim
 * <p/>
 * 0 Long   hash
 * 1 JPO    linkTuple
 * 2 Long   distance
 * 3 Integer   count
 * 4 Long   distSquared #
 * 5 Double CPA
 * -- 6 Long   minDist #
 * --  7 Long   maxDist #
 * -- 8 Double median  #
 */
public class WikiSimResult extends Tuple6<
        Long, // hash
        String, // page A
        String, // page B
        Long, // distance
        Integer, // count
//        Long, // distSquared
//        Long, // min
//        Long, // max
        DoubleListValue // CPA
        > {
    private final boolean enableMinMax = false;
    private final boolean enableDistSquared = false;
//    private final boolean enableCount = false;

    private final static int CPA_LIST_KEY = 5;
    private final static int MAX_KEY = 7; // disabled
    private final static int MIN_KEY = 6; // disabled
    private final static int DISTSQUARED_KEY = 5; // disabled
    private final static int COUNT_KEY = 4;
    private final static int DISTANCE_KEY = 3;

    public WikiSimResult() {
        // Flink needs empty constructor
    }

    public WikiSimResult(LinkTuple link, int distance) {

        setField(link.getHash(), 0);
        setField(link.getFirst(), 1);
        setField(link.getSecond(), 2);

//        setField(link, 1);

        setDistance(distance);
        init();
    }

    public WikiSimResult(String pageA, String pageB, int distance) {

        setField(LinkTuple.getHash(pageA, pageB), 0);
        setField(pageA, 1);
        setField(pageB, 2);

        setDistance(distance);
        init();
    }

    public void init() {
        setCount(1);
        setDistSquared(0);
        setMin(0);
        setMax(0);
        setMedian(.0);
        setField(new DoubleListValue(), CPA_LIST_KEY);
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
        if (enableDistSquared)
            setField(distSquared, DISTSQUARED_KEY);
    }

    public void setCPA(DoubleListValue cpa) {
        setField(cpa, CPA_LIST_KEY);
    }

    public void setCPA(double[] cpa) {
        setField(DoubleListValue.valueOf(cpa), CPA_LIST_KEY);
    }

    public void setMin(long min) {
        if (enableMinMax)
            setField(min, MIN_KEY);
    }

    public void setMax(long max) {
        if (enableMinMax)
            setField(max, MAX_KEY);
    }

    public void setMedian(double median) {
//        setField(median, 8);
    }

    public long getHash() {
        return getField(0);
    }

//    public LinkTuple getLinkTuple() {
//        return getField(1);
//    }

    public long getDistance() {
        return getField(DISTANCE_KEY);
    }

    public int getCount() {
        return getField(COUNT_KEY);
    }

    public long getDistSquared() {
        if (enableDistSquared)
            return getField(DISTSQUARED_KEY);
        else
            return 0;
    }

    public DoubleListValue getCPA() {
        return getField(CPA_LIST_KEY);
    }

    public double getCPA(int alphaKey) {
        return ((DoubleListValue) getField(CPA_LIST_KEY)).get(alphaKey).getValue();
    }

    public long getMin() {
        if (enableMinMax)
            return getField(MIN_KEY);
        else
            return 0;
    }

    public long getMax() {
        if (enableMinMax)
            return getField(MAX_KEY);
        else
            return 0;
    }
}
