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

import cc.clabs.stratosphere.mlp.types.WikiPage;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import eu.stratosphere.nephele.configuration.Configuration;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author rob
 */
@ConstantFields(fields={})
@OutCardBounds(lowerBound=0, upperBound=OutCardBounds.UNBOUNDED)
public class SentenceEmitter extends MapStub {
    
    MaxentTagger tagger = null;
    
    private static final Log LOG = LogFactory.getLog( SentenceEmitter.class );
    
    // initialize reusable mutable objects
    private final PactRecord output = new PactRecord();
    private final PactString string = new PactString();
    private WikiPage page = null;

    @Override
    public void open(Configuration parameter) throws Exception {
      super.open( parameter );
      tagger = new MaxentTagger( parameter.getString( "MODEL", "models/wsj-0-18-left3words-distsim.tagger") );
    }
    
    @Override
    public void map( PactRecord record, Collector<PactRecord> collector ) {
        page = (WikiPage) record.getField( 0, WikiPage.class );
        
        // skip pages from namespaces other than
        if ( page.ns != 0 ) return;
        
        LOG.info( "Analysing Page '"+ page.title +"' (id: "+page.id+")" );
        /*
        List<List<HasWord>> sentences = MaxentTagger.tokenizeText( new StringReader( corpus ) );
        for (List<HasWord> sentence : sentences) {
            ArrayList<TaggedWord> tSentence = tagger.tagSentence(sentence);
            string.setValue( Sentence.listToString(tSentence, false) );
            output.setField( 0, string );
            collector.collect( output );
        }
        */
        
        string.setValue( page.getPlainText() );
        output.setField( 0, string );
        collector.collect( output );
        
    }
    
}
