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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author rob
 */
public final class SerializationUtils {
    
    private static byte[] buffer;
    private static int length;
    
    /**
     * Helper function to read the next string from a stream.
     * 
     * @param in the input stream
     * @return the extracted string
     * @throws IOException 
     */
    public static String readNextString( DataInput in ) throws IOException {
        length = in.readInt();
        buffer = new byte[ length ];
        in.readFully( buffer );
        return buffer.toString();
    }
    
    
    /**
     * Helper function to write a string into a given stream
     * 
     * @param out the output stream
     * @param string the string to be written
     * @throws IOException 
     */
    public static void writeString( DataOutput out, String string ) throws IOException {
        out.writeInt( string.length() );
        out.writeUTF( string );
    }
}
