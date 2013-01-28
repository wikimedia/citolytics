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

import cc.clabs.stratosphere.mlp.types.PactIdentifiers;
import cc.clabs.stratosphere.mlp.types.WikiDocument;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author rob
 */
public class DocumentProcessor extends MapStub {
        
    private static final Log LOG = LogFactory.getLog( DocumentProcessor.class );
   
    @Override
    public void map( PactRecord record, Collector<PactRecord> collector ) {
        
        WikiDocument doc = (WikiDocument) record.getField( 0, WikiDocument.class );
        // skip pages from namespaces other than
        if ( doc.getNS() != 0 ) return;
        
        LOG.info( "Analysing Page '"+ doc.getTitle() +"' (id: "+ doc.getId() +")" );
                
        // populate the list of known identifiers
        PactIdentifiers list = new PactIdentifiers();
        for ( String var : doc.getKnownIdentifiers() )
            list.add( new PactString( var ) );
        
        LOG.info( "Found Identifiers: "+ list );
        
        
        // finally emit all parts
        PactRecord output = new PactRecord();
        output.setField( 0, new PactInteger( doc.getId() ) );
        output.setField( 1, new PactString( doc.getPlainText() ) );
        output.setField( 2, list );
        
        collector.collect( output );   
    }
}