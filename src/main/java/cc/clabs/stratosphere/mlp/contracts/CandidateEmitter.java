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
import cc.clabs.stratosphere.mlp.types.PactSentence;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author rob
 */
public class CandidateEmitter extends CoGroupStub {
        
    private static final Log LOG = LogFactory.getLog( CandidateEmitter.class );
    
    @Override
    public void coGroup( Iterator<PactRecord> left, Iterator<PactRecord> right, Collector<PactRecord> collector ) {
        
        // populating identifier list
        ArrayList<PactString> identifiers = new ArrayList<>();
        // we'l allways get one record from the left,
        // therefore, we don't need to iterate through
        // left
        Iterator<PactString> it = left.next().getField( 2, PactIdentifiers.class ).iterator();
        while ( it.hasNext() ) identifiers.add( it.next() );
        

        // populating sentences list
        ArrayList<PactSentence> sentences =  new ArrayList<>();
        while ( right.hasNext() )
            // we need to clone the sentence objects, because of reused objects
            sentences.add( (PactSentence) right.next().getField( 1, PactSentence.class ).clone() );

        PactRecord output = new PactRecord();
        
        for ( PactString identifier : identifiers ) {
            for ( PactSentence sentence : sentences ) {
                if ( sentence.containsWord( identifier ) ) {
                    //output the sentence
                    output.setField( 0, identifier );
                    output.setField( 1, sentence );
                    collector.collect( output );
                }
            }
        }
        
    }
    
}