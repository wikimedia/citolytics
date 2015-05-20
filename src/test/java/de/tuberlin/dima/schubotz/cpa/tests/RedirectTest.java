package de.tuberlin.dima.schubotz.cpa.tests;


import de.tuberlin.dima.schubotz.cpa.types.list.DoubleListValue;
import org.apache.flink.types.DoubleValue;
import org.junit.Test;

public class RedirectTest {
    @Test
    public void DoubleListSumA() throws Exception {
        DoubleListValue a = DoubleListValue.valueOf(new double[]{1, 10, 100});
        DoubleListValue b = DoubleListValue.valueOf(new double[]{2, 20, 200});

        DoubleListValue sum = DoubleListValue.sum(a, b);

        if (sum.get(0).getValue() != 3
                || sum.get(1).getValue() != 30
                || sum.get(2).getValue() != 300) {
            throw new Exception("DoubleListValue.sum() does not work: " + sum);
        }
    }

    // double is not accurate !!!
    public void DoubleListSumB() {
        System.out.println(DoubleListValue.valueOf(new double[]{0.1 + 0.2}));
        System.out.println(new DoubleValue(0.3));

        double d = 0.1 + 0.2;
        System.out.println(d);
    }
}
