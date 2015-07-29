package de.tuberlin.dima.schubotz.wikisim.seealso.operators;

import de.tuberlin.dima.schubotz.wikisim.seealso.types.ResultRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by malteschwarzer on 23.01.15.
 * <p/>
 * 3<String, String, SORT
 * 3<String, String, SORT>
 */

@Deprecated
public class ListMapper<SORT extends Comparable> implements MapFunction<Tuple3<String, String, SORT>, ResultRecord<SORT>> {

    @Override
    public ResultRecord<SORT> map(Tuple3<String, String, SORT> in) throws Exception {
        return new ResultRecord<SORT>(
                (String) in.getField(0), (String) in.getField(1), (SORT) in.getField(2)
        );
    }

}
