package de.tuberlin.dima.schubotz.cpa.types;

import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.types.*;

public class DataTypes {

    public static class ResultList extends ListValue<IntValue> {
        private static final long serialVersionUID = 1L;

        public String toString() {
            // removes brackets
            return super.toString().substring(1, super.toString().length() - 1);
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
}
