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

import edu.stanford.nlp.ling.TaggedWord;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.StringValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author rob
 */
public class PactWord implements Key {
    
    
    /*
     * 
     */
    private StringValue word = new StringValue();
    
    /*
     * 
     */
    private StringValue tag = new StringValue();
    
    /**
     * default constructor
     */
    public PactWord() { }
    
    
    /**
     * 
     * @param word
     * @param tag 
     */
    public PactWord( final String word, final String tag ) {
        this.word.setValue( word );
        this.tag.setValue( tag );
    }
    
    /**
     * Constructor for PactWord. Replaces some odd conversions
     * from the Stanford Tagger.
     * 
     * @param word a TaggedWord (@see edu.stanford.nlp.ling.TaggedWord)
     */
    public PactWord( TaggedWord word ) {
        String v = word.value();
        String t = word.tag();
        if ( v.equals( "-LRB-" ) ) v = "(";
        if ( v.equals( "-RRB-" ) ) v = ")";
        if ( v.equals( "-LCB-" ) ) v = "{";
        if ( v.equals( "-RCB-" ) ) v = "}";
        if ( t.equals( "``" ) )    t = "\"";
        if ( t.equals( "''" ) )    t = "\"";
        if ( v.equals( "``" ) )    v = "\"";
        if ( v.equals( "''" ) )    v = "\"";
        if ( v.equals( "--" ) )    v = "-";
        this.setWord( v );
        this.setTag( t );
    }
    
    /**
     * Returns this PactWord as a TaggedWord from the Stanford
     * NLP Project (@see edu.stanford.nlp.ling.TaggedWord).
     * 
     * @return a TaggedWord
     */
    public TaggedWord getTaggedWord() {
        return new TaggedWord( word.getValue(), tag.getValue() );
    }
    
    /**
     * Returns the string representation of the word.
     * 
     * @return the string representation of the word
     */
    public String getWord() {
        return word.getValue();
    }
    
    /**
     * Sets the value of the word.
     * 
     * @param string the string representation of the word
     */
    public final void setWord( final String string ) {
        word.setValue( string );
    }
    
    /**
     * Returns the assigned tag value.
     * 
     * @return the assigned tag value
     */
    public String getTag() {
        return tag.getValue();
    }
    
    /**
     * Sets the tag to a given string value.
     * 
     * @param string a given string value
     */
    public final void setTag( final String string ) {
        tag.setValue( string );
    }

    @Override
    public void write( final DataOutput out ) throws IOException {
        word.write( out );
        tag.write( out );
    }

    @Override
    public void read( final DataInput in ) throws IOException {
        word.read( in );
        tag.read( in );
    }

    @Override
    public int compareTo( Key o ) {
        PactWord other = (PactWord) o;
        if (  this.word.equals( other.word ) &&  this.tag.equals( other.tag ) )
            return 0;
        else
            return this.word.compareTo( other.word );
    }
    
    @Override
    public String toString() {
        return this.getWord();
    }

}
