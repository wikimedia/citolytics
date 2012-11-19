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
package cc.clabs.stratosphere.mlp.io;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author rob
 */
public class XMLChunkParser extends TextInputFormat  {

    @Override
    public void configure (Configuration parameter) {
        super.configure( parameter );
        this.delimiter =  "</page>".getBytes();
    }
    
    /* (non-Javadoc)
     * @see eu.stratosphere.pact.common.io.DelimitedInputFormat#readRecord(eu.stratosphere.pact.common.type.PactRecord, byte[], int)
     */
    @Override
    public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
        super.readRecord( target, bytes, offset, numBytes );
        String content = target.getField( 0, PactString.class ).getValue();
        Pattern p = Pattern.compile( "<title>(.*?)</title>" );
        Matcher m = p.matcher( content );
        if ( m.find() && m.group( 1 ) != null )
            target.setField( 0, new PactString(  m.group( 1 ) ) );
        target.setField( 1, new PactString( this.filePath.toString() ));
        return true;
    }

}