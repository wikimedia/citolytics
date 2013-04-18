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

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author rob
 */
public final class PactRelation implements Key {

    private PactDouble score = new PactDouble();
    private PactInteger iposition = new PactInteger();
    private PactInteger wposition = new PactInteger();
    private PactString identifier = new PactString();
    private PactSentence sentence = new PactSentence();
    private PactInteger id = new PactInteger();
    
    /**
     * 
     * @return 
     */
    public PactDouble getScore() {
        return score;
    }
    
    
    /**
     * 
     * @param score 
     */
    public void setScore( PactDouble score ) {
        this.score = score;
    }
    
    
    /**
     * 
     * @param score 
     */
    public void setScore( Double score ) {
        this.score = new PactDouble( score );
    }
    
    /**
     * 
     * @return 
     */
    public PactString getIdentifier() {
        return identifier;
    }
    
    
    /**
     * 
     * @param identifier 
     */
    public void setIdentifier( PactString identifier ) {
        this.identifier = identifier;
    }
    
    
    /**
     * 
     * @param identifier 
     */
    public void setIdentifier( String identifier ) {
        setIdentifier( new PactString( identifier ) );
    }
    
    
    /**
     * 
     * @return 
     */
    public PactInteger getIdentifierPosition() {
        return iposition;
    }
    
    
    /**
     * 
     * @param position 
     */
    public void setIdentifierPosition( PactInteger position ) {
        this.iposition = position;
    }
    
    
    /**
     * 
     * @param position 
     */
    public void setIdentifierPosition( Integer position ) {
        setIdentifierPosition( new PactInteger( position ) );
    }
    
    
    /**
     * 
     * @return 
     */
    public PactInteger getWordPosition() {
        return wposition;
    }
    
    
    /**
     * 
     * @param position 
     */
    public void setWordPosition( PactInteger position ) {
        this.wposition = position;
    }
    
    
    /**
     * 
     * @param position 
     */
    public void setWordPosition( Integer position ) {
        setWordPosition( new PactInteger( position ) );
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
    public PactInteger getId () {
        return this.id;
    }
    
    public void setId( PactInteger id ) {
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
