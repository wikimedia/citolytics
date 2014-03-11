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

import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author rob
 */
public class PactFormula implements Value {
    
    
    /*
     * 
     */
    private StringValue hash = new StringValue();
    
    /*
     * 
     */
    private StringValue src = new StringValue();
    
    /**
     * default constructor
     */
    public PactFormula() { }
    
    
    /**
     * 
     * @param hash
     * @param src 
     */
    public PactFormula( final String hash, final String src ) {
        this.hash.setValue( hash );
        this.hash.setValue( src );
    }
    
    /**
     * Returns the string representation of the hash.
     * 
     * @return the string representation of the hash
     */
    public String getHash() {
        return hash.getValue();
    }
    
    /**
     * Sets the value of the hash.
     * 
     * @param string the string representation of the hash
     */
    public final void setHash( final String string ) {
        hash.setValue( string );
    }
    
    /**
     * Returns the source of the formula.
     * 
     * @return source of the formula (e.g. latex)
     */
    public String getSrc() {
        return src.getValue();
    }
    
    /**
     * Sets the source to a given string value.
     * 
     * @param string a given string value
     */
    public final void setSrc( final String string ) {
        src.setValue( string );
    }

    @Override
    public void write( final DataOutput out ) throws IOException {
        hash.write( out );
        src.write( out );
    }

    @Override
    public void read( final DataInput in ) throws IOException {
        hash.read( in );
        src.read( in );
    }
    
    @Override
    public String toString() {
        return this.getHash();
    }

}
