package de.tuberlin.dima.schubotz.cpa.types;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.types.*;

public class DataTypes {

    public static class ResultList extends ListValue<IntValue> {
        private static final long serialVersionUID = 1L;

        public String toString() {
            // removes brackets
            return super.toString().substring(1, super.toString().length() - 1);
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

    /**
     * linkTuple, distance, count, distSquared, recDistα, min, max, distanceList, median
     */
    public static class Result extends Tuple9<LinkTuple, Integer, Integer, Long, Double, Integer, Integer, ResultList, Double> {
        private static final long serialVersionUID = 1L;

        public Result() {

        }

        public Result(LinkTuple link, Integer distance, Integer count) {
            setField(link, 0);
            setField(distance, 1);
            setField(count, 2);
            setField(new Long(0), 3);
            setField(new Double(0), 4);
            setField(0, 5);
            setField(0, 6);
            setField(new ResultList(), 7);
            setField(new Double(0), 8);
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

    /**
     * linkTuple, distance, count, distSquared, recDistα, min, max, distanceList, median
     */
    public static class ResultFull extends Tuple9<LinkTuple, Integer, Integer, Long, Double, Integer, Integer, ResultList, Double> {
        private static final long serialVersionUID = 1L;

        public ResultFull() {

        }

        public ResultFull(LinkTuple link, Integer distance, Integer count) {
            setField(link, 0);
            setField(distance, 1);
            setField(count, 2);
            setField(new Long(0), 3);
            setField(new Double(0), 4);
            setField(0, 5);
            setField(0, 6);
            setField(new ResultList(), 7);
            setField(new Double(0), 8);
        }
    }
}
