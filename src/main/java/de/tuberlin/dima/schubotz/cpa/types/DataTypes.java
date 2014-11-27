package de.tuberlin.dima.schubotz.cpa.types;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.types.*;

import java.math.BigDecimal;

public class DataTypes {

    public static class ResultList extends ListValue<IntValue> {
        private static final long serialVersionUID = 1L;

        public String toString() {
            // removes brackets
            return super.toString().substring(1, super.toString().length() - 1);
        }
    }

    /**
     * hash, linkTuple, distance, count, distSquared, recDistα, min, max, distanceList, median
     */
    public static class Result extends Tuple9<Long, LinkTuple, Long, Long, Long, Double, Long, Long, Double> {
        private static final long serialVersionUID = 1L;

        public Result() {

        }

        public Result(LinkTuple link, int distance, int count) {

            setField(link.getHash(), 0);
            setField(link, 1);

            setField(Long.valueOf(distance), 2);
            setField(Long.valueOf(count), 3); //  count

            setField(new Long(0), 4);
            setField(.0, 5);
            setField(new Long(0), 6);
            setField(new Long(0), 7);
            //setField(new ResultList(), 7);
            setField(.0, 8);

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


    public static class HistogramResult extends Tuple4<Integer, Integer, Integer, Long> {
        private static final long serialVersionUID = 1L;

        public HistogramResult() {

        }

        public HistogramResult(int ns, int articleCount, int linkCount, long linkpairCount) {
            setField(ns, 0);
            setField(articleCount, 1);
            setField(linkCount, 2);
            setField(linkpairCount, 3);
        }
    }

    public static class MapperResult extends Tuple3<LinkTuple, Integer, Integer> {
        private static final long serialVersionUID = 1L;

        public MapperResult() {
        }

        public MapperResult(LinkTuple link, int distance, int count) {
            setField(link, 0);
            setField(distance, 1);
            setField(count, 2);
        }
    }

}
