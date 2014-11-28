package de.tuberlin.dima.schubotz.cpa.types;

import java.math.BigDecimal;

/**
 * Wrapper class for BigDecimal. Flink requieres public nullary constructor.
 */
public class WikiSimBigDecimal {
    public BigDecimal value;

    public WikiSimBigDecimal() {
        value = new BigDecimal(0);
    }

    public WikiSimBigDecimal(BigDecimal v) {
        value = v;
    }

    public WikiSimBigDecimal add(WikiSimBigDecimal augend) {
        value.add(augend.value);

        return this;
    }

    public String toString() {
        return value.toString();
    }
}
