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

import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class DocumentProcessor extends MapFunction {

    // private static final Log LOG = LogFactory.getLog(DocumentProcessor.class);

    private Double α;

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);
        α = Double.parseDouble(parameter.getString("α", "1.5"));
    }

    @Override
    public void map(Record record, Collector<Record> collector) {

        WikiDocument doc = record.getField(0, WikiDocument.class);
        doc.collectLinks(collector, α);
        //LOG.info( "Analyzed Page '"+ doc.getTitle() );
    }
}








