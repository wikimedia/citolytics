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
package cc.clabs.stratosphere.mlp.contracts;

import cc.clabs.stratosphere.mlp.types.WikiDocument;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 *
 * @author rob
 */
@ConstantFields(fields={})
@OutCardBounds(lowerBound=0, upperBound=OutCardBounds.UNBOUNDED)
public class DocumentAnalyser extends MapStub {
        
    private static final Log LOG = LogFactory.getLog( DocumentAnalyser.class );
    
    // initialize reusable mutable objects
    private final PactRecord output = new PactRecord();
    private final PactString line = new PactString();
    private final PactInteger number = new PactInteger();
    private WikiDocument doc = null;
   
    @Override
    public void map( PactRecord record, Collector<PactRecord> collector ) {
        doc = (WikiDocument) record.getField( 0, WikiDocument.class );
        // skip pages from namespaces other than
        if ( doc.getNS() != 0 ) return;
        
        LOG.info( "Analysing Page '"+ doc.getTitle() +"' (id: "+ doc.getId() +")" );
        LOG.info( "Found Identifiers: "+ doc.getKnownIdentifiers() );
        
        line.setValue( doc.getPlainText() );
        number.setValue( doc.getId() );
        
        output.setField( 0, number );
        output.setField( 1, line );
        collector.collect( output );   
    }
}