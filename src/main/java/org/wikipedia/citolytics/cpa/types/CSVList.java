package org.wikipedia.citolytics.cpa.types;

import org.apache.commons.lang.StringUtils;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;

import java.util.ArrayList;

/**
 * Override the toString method to use CSV output (| delimiter). This makes reading of WikiSim output easier.
 */
public class CSVList<T> extends ArrayList<T> {
    public String toString() {
        return StringUtils.join(this, WikiSimConfiguration.csvFieldDelimiter);
    }
}
