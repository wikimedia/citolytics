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

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

import java.io.StringReader;
import java.util.List;

/**
 *
 * @author rob
 */
public class SentenceEmitter extends MapFunction {
    
    /**
     * 
     */
    private MaxentTagger tagger = null;
        
    /**
     * 
     */
    private final Record target = new Record();

    
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
    public void map( Record record, Collector<Record> collector ) {
        target.clear();
        // field 0 remains the same (id of the document)
        target.setField( 0, record.getField( 0, IntValue.class ) );
        String plaintext = record.getField( 1, StringValue.class ).getValue();
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
            target.setField( 2, new DoubleValue( (double) position / (double) tokenized.size() ) );
            collector.collect( target );
        }
    }
}
