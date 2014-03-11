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
package cc.clabs.stratosphere.mlp.types;

import eu.stratosphere.types.ListValue;
import eu.stratosphere.types.StringValue;

import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author rob
 */
public class PactSentence extends ListValue<PactWord> implements Cloneable {
    
    /**
     * 
     * @param word
     * @return 
     */
    public ArrayList<Integer> getWordPosition( String word ) {
        ArrayList<Integer> positions = new ArrayList<>();
        String token;
        Integer pos = -1;
        Iterator<PactWord> it = this.iterator();
        while ( it.hasNext() ) {
            pos += 1;
            token = it.next().getWord();
            if ( token.equals( word ) )
                positions.add( pos );
        }
        return positions;        
    }
    
    
    /**
     *
     * @return 
     */
    public boolean containsWord( String word ) {
        return !getWordPosition( word ).isEmpty();
    }

    /**
     * 
     * @param word
     * @return 
     */
    public boolean containsWord( StringValue word ) {
        return containsWord( word.getValue() );
    }
    
    /**
     * 
     * @param word
     * @return 
     */
    public boolean containsWord( PactWord word ) {
        return containsWord( word.getWord() );
    }
    
  
    
    @Override
    public Object clone() {
        PactSentence obj = new PactSentence();
        obj.addAll( this );
        return obj;
    }
    
    @Override
    public String toString() {
        String buffer = "";
        Iterator<PactWord> it = this.iterator();
        while ( it.hasNext() ) buffer += it.next().getWord() + " ";
        return buffer;
    }
    
}
