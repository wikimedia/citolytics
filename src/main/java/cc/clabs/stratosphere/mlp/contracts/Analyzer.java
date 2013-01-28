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
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author rob
 */
public class Analyzer extends ReduceStub {
        
    private static final Log LOG = LogFactory.getLog( Analyzer.class );
    
    
    private final static List<String> blacklist = Arrays.asList(
        "function", "functions",
        "behavior", "infinity", "sum", "other",
        "equation", "equations",
        "value", "values",
        "solution", "solutions",
        "result", "results"
    );
    
    private Double α;
    private Double β;
    private Double γ;
    private Double threshold;

    @Override
    public void open(Configuration parameter) throws Exception {
      super.open( parameter );
      α = Double.parseDouble( parameter.getString( "α", "1" ) );
      β  = Double.parseDouble( parameter.getString( "β", "1" ) );
      γ = Double.parseDouble( parameter.getString( "γ", "1" ) );
      threshold = Double.parseDouble( parameter.getString( "THRESHOLD", "0.8" ) );
    }

    @Override
    public void reduce( Iterator<PactRecord> iterator, Collector<PactRecord> collector ) throws Exception {
        String identifier = null;
        PactRecord record = null;
        HashMap<String,Integer> ω = new HashMap<>();
        Integer Ω = 0;
        ArrayList<PactSentence> sentences =  new ArrayList<>();
        
        /*                         _
         *                        / |
         *   ____ ___ ___ ______  - |
         *  /  ._|   ) __|  __  ) | |
         * ( () ) | |> _) | || |  | |
         *  \__/   \_)___)|_||_|  |_|
         * calculate the word frequencies for sentences
         */
        while ( iterator.hasNext() ) {
            record = iterator.next();
            
            // although the first field contains always the same value,
            // we'll read it for the sake of simplicity.
            identifier = record.getField( 0, PactString.class ).getValue();
            PactSentence sentence = record.getField( 1, PactSentence.class );
            
            for ( PactWord word : sentence ) {
                // only count words we're interested in
                if ( filterWord( word, identifier ) ) continue;
                Integer count = ( ω.containsKey( word.getWord() ) ) ?
                        ω.get( word.getWord() ) + 1 : 1;
                // update the maximun token frequency
                Ω = Math.max( count, Ω );
                ω.put( word.getWord(), count );
            }            
            // put clones of all sentences into a new collection
            sentences.add( (PactSentence) sentence.clone() );
        }
        
        /*                        ____
         *                       (___ \
         *  ____ ___ ___ ______    __) )
         * /  ._|   ) __|  __  )  / __/
         *( () ) | |> _) | || |  | |___
         * \__/   \_)___)|_||_|  |_____)
         * the kernel step
         */
        for ( Iterator<PactSentence> it = sentences.iterator(); it.hasNext(); ) {
            PactSentence sentence = it.next();
            
            ArrayList<Integer> positions = sentence.getWordPosition( identifier );
            
            Integer position = -1;
            for ( PactWord word : sentence ) {
                position += 1;
                if ( filterWord( word, identifier ) ) continue;
                
                Integer dist = getMinimumDistance( position, positions );
                Double score = getScore( dist, ω.get( word.getWord() ), Ω );
                                
                // if the score is lesser than our minimum threshold
                // we'll continue with the next word
                if ( score < threshold ) continue;
                                
                // otherwise emit a match                
                record.clear();
                record.setField( 0, new PactString( identifier ) );
                record.setField( 1, new PactDouble( score ) );
                record.setField( 2, new PactInteger( position ) );
                record.setField( 3, word );
                record.setField( 4, sentence );
                collector.collect( record );
            }
        }
    }
    
    
    /**
     * 
     * @param pos
     * @param positions
     * @return 
     */
    private Integer getMinimumDistance( Integer pos, ArrayList<Integer> positions ) {
        Integer min = Integer.MAX_VALUE;
        for ( Integer position : positions )
            min = Math.min( min, Math.abs( position - pos ) );
        return min;
    }
    
    /**
     * 
     * @param distance
     * @param ω
     * @param Ω
     * @return 
     */
    private Double getScore( Integer distance, Integer ω, Integer Ω ) {        
        Double dist = Math.pow( Math.E, ( 1d - distance ) / 6d );
        Double freq = (double) ( ω / Ω );
        Double seq = 0d;
        return ( α * dist + β * freq + γ * seq ) /
               ( α + β + γ );
    }

    /**
     * 
     * @param word
     * @param identifier
     * @return 
     */
    private boolean filterWord( PactWord word, String identifier ) {
               // skip the identifier words
        return word.getWord().equals( identifier ) ||
               // skip blacklisted words
               blacklist.contains( word.getWord() ) ||
               // we're only interested in nouns, adjectives and entities
               !word.getTag().matches( "NN[PS]{0,2}|ENTITY|JJ" );
    }
    
}