/*        __
 *        \ \
 *   _   _ \ \  ______
 *  | | | | > \(  __  )
 *  | |_| |/ ^ \| || |
 *  | ._,_/_/ \_\_||_|
 *  | |
 *  |_|
 * 
 * ----------------------------------------------------------------------------
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <rob ∂ CLABS dot CC> wrote this file. As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.
 * ----------------------------------------------------------------------------
 */
package de.tuberlin.dima.schubotz.cpa.contracts;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

import java.util.Iterator;

@ReduceOperator.Combinable
public class calculateCPA extends ReduceFunction {

    private Integer threshold;
    private Double α;

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open( parameter );
        α = Double.parseDouble(parameter.getString("α", "1.5"));
        threshold = parameter.getInteger("THRESHOLD", 1);
    }

    private IntValue v2F(int in) {
        return new IntValue(in);
    }

    private DoubleValue v2F(double in) {
        return new DoubleValue(in);
    }
    /*
    Schema
                    0 target.addField(linkTuple);
                    1 target.addField(distance);
                    2 target.addField(count);
                    3 distSquared
                    4 recDistα
                    5 min
                    6 max
     */
    private void internalReduce(Iterator<Record> iterator, Collector<Record> collector, Integer minOut) {
        Record rec = null;
        int cnt = 0;
        int distance = 0;
        int min = 0;
        int max = 0;
        int distSquared = 0;
        double recDistα = 0.;
        while (iterator.hasNext()) {
            rec = iterator.next();
            int d = rec.getField(1, IntValue.class).getValue();
            int c = rec.getField(2, IntValue.class).getValue();
            distance += d;
            cnt += c;
            if (rec.getNumFields() > 2) {
                distSquared += rec.getField(3, IntValue.class).getValue();
                recDistα += rec.getField(4, DoubleValue.class).getValue();
                min = Math.min(min, rec.getField(5, IntValue.class).getValue());
                max = Math.max(max, rec.getField(6, IntValue.class).getValue());
            } else {
                min = Math.min(min, d);
                max = Math.max(max, d);
                distSquared += d * d;
                recDistα += Math.pow(d, α);
            }
        }
        if (cnt > minOut) {
            assert rec != null;
            rec.setField(1, v2F(distance));
            rec.setField(2, v2F(cnt));
            if (rec.getNumFields() > 2) {
                rec.addField(v2F(distSquared));
                rec.addField(v2F(recDistα));
                rec.addField(v2F(min));
                rec.addField(v2F(max));
                //rec.addField();
            } else {
                rec.setField(3, v2F(distSquared));
                rec.setField(4, v2F(recDistα));
                rec.setField(5, v2F(min));
                rec.setField(6, v2F(max));
            }
            collector.collect(rec);
        }
    }

    @Override
    public void reduce(Iterator<Record> iterator, Collector<Record> collector) {
        internalReduce(iterator, collector, threshold);
    }

    @Override
    public void combine(Iterator<Record> iterator, Collector<Record> collector) {
        internalReduce(iterator, collector, 0);
    }
}