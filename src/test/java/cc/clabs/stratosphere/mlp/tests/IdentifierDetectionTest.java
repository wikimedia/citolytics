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
package cc.clabs.stratosphere.mlp.tests;

import cc.clabs.stratosphere.mlp.utils.TexIdentifierExtractor;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rob
 */
public class IdentifierDetectionTest {
    
    @Test
    public void singleIdentifiers () {
        ArrayList<String> detected;
        
        String x = "x";
        detected = TexIdentifierExtractor.getAll( x,false );
        assertEquals( "Number of identifiers mismatch (should be 1)", 1, detected.size() );
        assertEquals( "Extracted identifier did not match", x, detected.get( 0 ) );
        
        String xx = "x + x^{2}";
        detected = TexIdentifierExtractor.getAll( xx,false );
        assertEquals( "Number of identifiers mismatch (should be 1)", 1, detected.size() );
        assertEquals( "Extracted identifier did not match", x, detected.get( 0 ) );
    }
}
