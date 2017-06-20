package org.wikipedia.citolytics.cpa.types;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Represents a recommendation pair for pages A <-> B
 *
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
public class RecommendationPair extends Tuple8<
        Long, // hash
        String, // page title A
        String, // page title B
        Double, // distance
        Integer, // count
//        Long, // distSquared
//        Long, // min
//        Long, // max
        List<Double>, // CPA
        Integer, // page id A
        Integer // page id B
        > {
    private final boolean enableMinMax = false;
    private final boolean enableDistSquared = false;
//    private final boolean enableCount = false;

    public final static int HASH_KEY = 0;
    public final static int PAGE_A_KEY = 1;
    public final static int PAGE_B_KEY = 2;
    public final static int PAGE_A_ID_KEY = 6;
    public final static int PAGE_B_ID_KEY = 7;
    private final static int CPI_LIST_KEY = 5;
    private final static int MAX_KEY = 7; // disabled
    private final static int MIN_KEY = 6; // disabled
    private final static int DISTSQUARED_KEY = 5; // disabled
    private final static int COUNT_KEY = 4;
    private final static int DISTANCE_KEY = 3;

    public RecommendationPair() {
        // Flink needs empty constructor
    }

    public RecommendationPair(LinkPair link, double distance) {
        init(link.getFirst(), link.getSecond(), distance, 1);
        init();
    }

    public RecommendationPair(String pageA, String pageB) {
        init(pageA, pageB, 0, 1);
        init();
    }

    public RecommendationPair(String pageA, String pageB, double distance, double[] cpi) {
        // TODO Completely remove distance-field
        init(pageA, pageB, distance, 1);
        init();

        setCPI(cpi);
    }

//    /**
//     * Use constructor with CPI values instead.
//     *
//     * @param pageA
//     * @param pageB
//     * @param distance
//     * @param alphas
//     */
//    @Deprecated
//    public RecommendationPair(String pageA, String pageB, double distance, double[] alphas) {
//
//        init(pageA, pageB, distance, 1);
//        init();
//
//        // set CPI for all alpha values
//        for (double alpha : alphas) {
//            getCPI().add(RecommendationPairExtractor.computeCPI(distance, alpha));
//        }
//    }


    public RecommendationPair(String pageA, String pageB, double distance, int count, double[] cpa) {

        init(pageA, pageB, distance, count);
        setCPI(cpa);
    }

    public void init(String pageA, String pageB, double distance, int count) {
        setField(LinkPair.getHash(pageA, pageB), HASH_KEY);
        setField(pageA, PAGE_A_KEY);
        setField(pageB, PAGE_B_KEY);
        setPageAId(0);
        setPageBId(0);

        setDistance(distance);
        setCount(count);
    }

    public void init() {
        setPageAId(0);
        setPageBId(0);
        setCount(1);
        setDistSquared(0);
        setMin(0);
        setMax(0);
        setMedian(.0);
//        setField(new DoubleListValue(), CPI_LIST_KEY);
        setField(new ArrayList<Double>(), CPI_LIST_KEY);

    }

    public void setDistance(double distance) {
        setField(distance, DISTANCE_KEY);
    }

//    public void setDistance(double distance) {
//        setField(Long.valueOf(distance), DISTANCE_KEY);
//    }

    public void setCount(int count) {
        setField(count, COUNT_KEY);
    }

    public void addCount(int count) {
        setCount(getCount() + count);
    }

    public void setDistSquared(long distSquared) {
        if (enableDistSquared)
            setField(distSquared, DISTSQUARED_KEY);
    }

    public void setCPI(List<Double> cpa) {
        setField(cpa, CPI_LIST_KEY);
    }

    public void setCPI(double[] cpi) {
        // DoubleListValue.valueOf(cpi)
        this.f5 = new ArrayList<>();
        this.f5.addAll(Arrays.asList(ArrayUtils.toObject(cpi)));

//        setField(Arrays.asList(ArrayUtils.toObject(cpi)), CPI_LIST_KEY);
    }

    public void addCPI(List<Double> cpi) throws Exception {
        setField(sum(cpi, getCPI()), CPI_LIST_KEY);
    }

    public static List<Double> sum(List<Double> firstList, List<Double> secondList) throws Exception {
        List<Double> result = new ArrayList<>();

        if (firstList == null || secondList == null) {
            throw new Exception("Cannot sum lists if one list NULL.");
        } else if (firstList.size() == 0 && secondList.size() == 0) {
            throw new Exception("Cannot sum lists if both lists are empty.");
        } else if (firstList.size() == 0 && secondList.size() > 0) {
            result = secondList;
        } else if (secondList.size() == 0 && firstList.size() > 0) {
            result = firstList;
        } else if (firstList.size() != secondList.size()) {
            throw new Exception("Cannot sum lists with different size.");
        }

        int i = 0;
        for (Double firstValue : firstList) {
            result.add(i, firstValue + secondList.get(i));
            i++;
        }

        return result;
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

//    public LinkPair getLinkTuple() {
//        return getField(1);
//    }

    public int getPageAId() {
        return getField(PAGE_A_ID_KEY);
    }

    public int getPageBId() {
        return getField(PAGE_B_ID_KEY);
    }

    public void setPageAId(int id) {
        setField(id, PAGE_A_ID_KEY);
    }

    public void setPageBId(int id) {
        setField(id, PAGE_B_ID_KEY);
    }

    public String getPageA() {
        return this.f1;
    }

    public String getPageB() {
        return this.f2;
    }

    public double getDistance() {
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

    public List<Double> getCPI() {
        return getField(CPI_LIST_KEY);
    }

    public double getCPI(int alphaKey) {
        return getCPI().get(alphaKey);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof RecommendationPair)) return false;

        RecommendationPair that = (RecommendationPair) o;

        return getPageA().equals(that.getPageA())
                && getPageB().equals(that.getPageB())
                && getCount() == that.getCount()
//                && getCPI().equals(that.getCPI())
                ;
    }

    @Override
    public int hashCode() {
        return getPageA().hashCode()
                + getPageB().hashCode()
                + getCount()
                + Double.valueOf(getDistance()).hashCode()
//                + getCPI().hashCode()
                ;
    }

    /**
     * Construct RecommendationPair tuple from string / csv line
     *
     * @param csv
     * @param delimitter
     * @return RecommendationPair
     */
    public static RecommendationPair valueOf(String csv, String delimitter) {
        String[] cols = csv.split(Pattern.quote(delimitter));

        RecommendationPair res = new RecommendationPair(cols[PAGE_A_KEY], cols[PAGE_B_KEY]);

        res.setDistance(Double.valueOf(cols[DISTANCE_KEY]));
        res.setCount(Integer.valueOf(cols[COUNT_KEY]));

//        DoubleListValue cpi = new DoubleListValue();
//        for (int i = CPI_LIST_KEY; i < cols.length; i++) {
//            cpi.add(new DoubleValue(Double.valueOf(cols[i])));
//        }
//        res.setCPI(cpi);

        List<Double> cpi = new ArrayList<>();
        for (int i = CPI_LIST_KEY; i < cols.length; i++) {
            cpi.add(Double.valueOf(cols[i]));
        }
        res.setCPI(cpi);

        return res;
    }
}
