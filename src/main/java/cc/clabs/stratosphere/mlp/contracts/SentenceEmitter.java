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

import cc.clabs.stratosphere.mlp.types.PactSentence;
import cc.clabs.stratosphere.mlp.types.PactWord;
import cc.clabs.stratosphere.mlp.utils.SentenceUtils;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

import java.io.StringReader;
import java.util.List;

/**
 *
 * @author rob
 */
public class SentenceEmitter extends MapStub {
    
    /**
     * 
     */
    private MaxentTagger tagger = null;
        
    /**
     * 
     */
    private final PactRecord target = new PactRecord();

    
    @Override
    public void open(Configuration parameter) throws Exception {
      super.open( parameter );
      String model = parameter.getString( "POS-MODEL", "models/wsj-0-18-left3words-distsim.tagger" );     
      // tokenizerOptions -> untokenizable=noneDelete
      // should be used to avoid tokenizing f(x) to f(xx  etc.
      // http://nlp.stanford.edu/nlp/javadoc/javanlp/edu/stanford/nlp/process/PTBTokenizer.html
      tagger = new MaxentTagger( model );
    }
    
    
    @Override
    public void map( PactRecord record, Collector<PactRecord> collector ) {
        target.clear();
        // field 0 remains the same (id of the document)
        target.setField( 0, record.getField( 0, PactInteger.class ) );       
        String plaintext = record.getField( 1, PactString.class ).getValue();        
        // tokenize the plaintext
        List<List<HasWord>> tokenized = MaxentTagger.tokenizeText( new StringReader( plaintext ) );
        Integer position = -1;    
        for ( List<HasWord> tokens :  tokenized ) {
            position += 1;
            // for each detected sentence
            PactSentence sentence = new PactSentence();
            // populate a wordlist/sentence
            for ( TaggedWord word : tagger.tagSentence( tokens ) )
                sentence.add( new PactWord( word ) );
            // postprocess the sentence
            sentence = SentenceUtils.joinByTagPattern( sentence, "\" * \"", "ENTITY" );
            sentence = SentenceUtils.replaceAllByTag( sentence, "ENTITY", "[\"]", "" );
            sentence = SentenceUtils.replaceAllByTag( sentence, "ENTITY", "^ | $", "" );
            sentence = SentenceUtils.replaceAllByPattern( sentence, "MATH[0-9A-F]+", "FORMULA" );
            // emit the final sentence
            target.setField( 1, sentence );
            target.setField( 2, new PactDouble( (double) position / (double) tokenized.size() ) );
            collector.collect( target );
        }
    }
}
