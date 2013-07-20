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
package cc.clabs.stratosphere.mlp.utils;

import cc.clabs.stratosphere.mlp.types.PactSentence;
import cc.clabs.stratosphere.mlp.types.PactWord;
import java.util.ArrayList;
import java.util.List;


/**
 *
 * @author rob
 */
public class SentenceUtils {
    
    public final static String WILDCARD = "*";
        
    public static PactSentence replaceAllByPattern( PactSentence sentence, String regex, String withTag ) {
        for ( PactWord word: sentence )
            if ( word.getWord().matches( regex ) )
                word.setTag( withTag );
        return sentence;
    }
    
    public static PactSentence replaceAllByTag( PactSentence sentence, String tag, String regex, String replacement ) {
        for ( PactWord word : sentence )
            // skip other words than those with a specific tag
            if ( word.getTag().equals( tag ) ) {
                String text = word.getWord();
                text = text.replaceAll( regex, replacement );
                word.setWord( text );
            }
        return sentence;
    }

    /**
     * Joins a set of PactWords, described by a pattern,
     * together with a given tag into a new PactWord within
     * the sentence. A pattern is a string containing tags
     * and wildcards where every part is separated by a space
     * character.
     * 
     * @param sentence  the sentence
     * @param pattern   a string containing the pattern
     * @param withTag   tag given to new PactWords
     * 
     * @return  a new sentence with the joined PactWords
     */
    public static PactSentence joinByTagPattern( PactSentence sentence, String pattern, String withTag ) {
        
        PactSentence result = new PactSentence();
        String[] tags = pattern.split( " " );
        
        // nothing to compare to
        if (  tags.length == 0 ) return sentence;
        // pattern can never be matched
        if ( tags.length > sentence.size() ) return sentence;        
        
        
        int pos = 0;
        String curTag = "", nexTag = "";
        ArrayList<PactWord> candidates = new ArrayList<>();
        
        // for every word …
        for ( PactWord word : sentence ) {
            // reset the position if needed
            if ( pos >= tags.length - 1 ){
                result.add( new PactWord( joinPactWords( candidates ), withTag ) );
                // reset the candidates
                candidates.clear();
                // and increment the position of pattern array
                pos = 0;
            }
            // get the tags form tags array
            curTag = tags[ pos ];
            nexTag = ( tags.length > pos + 1 ) ? tags[ pos + 1 ] : null;
                        
            // current tag is wildcard symbol
            if ( curTag.equals( WILDCARD ) ) {
                // when the tag of the next word matches
                if ( nexTag != null && word.getTag().equals( nexTag ) ) {
                    candidates.add( word );
                    pos += 1;
                // otherwise add the current word to the concat
                } else {
                    // concatenate the current word
                    candidates.add( word );
                }                
            }
            // the current tag matches the tag of the word
            else if ( curTag.equals( word.getTag() ) ) {
                // concatenate the current word
                candidates.add( word );
                // and increment the position of pattern array
                pos += 1;
            // current tag doesn't match
            } else {
                if ( !candidates.isEmpty() ) {
                    result.addAll( candidates );
                    candidates.clear();
                }
                pos = 0;
                result.add( word );
            }
        }
        // deal with a possible remainders
        if ( !candidates.isEmpty() ) {
            result.addAll( candidates );
            candidates.clear();
        }
        return result;
    }
    
    /**
     * 
     * @param candidates
     * @return 
     */
    private static String joinPactWords( List<PactWord> candidates ) {
        String result = "";
        for( PactWord word : candidates )
            result += word.getWord() + " ";
        return result.trim();
    }
    
}
