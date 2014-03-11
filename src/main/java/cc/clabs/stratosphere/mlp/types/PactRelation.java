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

import eu.stratosphere.types.Key;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author rob
 */
public final class PactRelation implements Key {

    private DoubleValue score = new DoubleValue();
    private IntValue iposition = new IntValue();
    private IntValue wposition = new IntValue();
    private StringValue identifier = new StringValue();
    private PactSentence sentence = new PactSentence();
    private IntValue id = new IntValue();
    
    /**
     * 
     * @return 
     */
    public DoubleValue getScore() {
        return score;
    }
    
    
    /**
     * 
     * @param score 
     */
    public void setScore( DoubleValue score ) {
        this.score = score;
    }
    
    
    /**
     * 
     * @param score 
     */
    public void setScore( Double score ) {
        this.score = new DoubleValue( score );
    }
    
    /**
     * 
     * @return 
     */
    public StringValue getIdentifier() {
        return identifier;
    }
    
    
    /**
     * 
     * @param identifier 
     */
    public void setIdentifier( StringValue identifier ) {
        this.identifier = identifier;
    }
    
    
    /**
     * 
     * @param identifier 
     */
    public void setIdentifier( String identifier ) {
        setIdentifier( new StringValue( identifier ) );
    }
    
    
    /**
     * 
     * @return 
     */
    public IntValue getIdentifierPosition() {
        return iposition;
    }
    
    
    /**
     * 
     * @param position 
     */
    public void setIdentifierPosition( IntValue position ) {
        this.iposition = position;
    }
    
    
    /**
     * 
     * @param position 
     */
    public void setIdentifierPosition( Integer position ) {
        setIdentifierPosition( new IntValue( position ) );
    }
    
    
    /**
     * 
     * @return 
     */
    public IntValue getWordPosition() {
        return wposition;
    }
    
    
    /**
     * 
     * @param position 
     */
    public void setWordPosition( IntValue position ) {
        this.wposition = position;
    }
    
    
    /**
     * 
     * @param position 
     */
    public void setWordPosition( Integer position ) {
        setWordPosition( new IntValue( position ) );
    }
    
    
    /**
     * 
     * @return 
     */
    public PactSentence getSentence() {
        return sentence;
    }
    
    
    /**
     * 
     * @param sentence 
     */
    public void setSentence( PactSentence sentence ) {
        this.sentence = sentence;
    }
    
    /**
     * 
     * @return 
     */
    public IntValue getId () {
        return this.id;
    }
    
    public void setId( IntValue id ) {
        this.id = id;
    }
    
    @Override
    public void write( DataOutput out ) throws IOException {
        iposition.write( out );
        identifier.write( out );
        wposition.write( out );
        sentence.write( out );
        score.write( out );
        id.write( out );
    }

    @Override
    public void read( DataInput in ) throws IOException {
        iposition.read( in );
        identifier.read( in );
        wposition.read( in );
        sentence.read( in );
        score.read( in );
        id.read( in );
    }

    @Override
    public int compareTo( Key o ) {
        PactRelation other = (PactRelation) o;
        // only and only if the identifier and the
        // sentences are equal, consider the relations
        // natural order as equal.
        if ( identifier.equals(other.getIdentifier() ) )
            if ( this.sentence.equals( other.getSentence() ) )
                return 0;
        // otherwise use the ordering derived from the
        // identifer
        return identifier.compareTo( other.identifier );
    }
    
    @Override
    public String toString() {
        String word = ((PactWord) sentence.get( wposition.getValue() )).getWord();
        return String.format( "%d | %-2.2s | %-5f | %-18.18s | %s",
            id.getValue(),
            identifier.getValue(),
            score.getValue(),
            word,
            sentence.toString() );
    }

}
