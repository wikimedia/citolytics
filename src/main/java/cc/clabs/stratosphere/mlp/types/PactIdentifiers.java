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

/**
 *
 * @author rob
 */
public final class PactIdentifiers extends ListValue<StringValue> {

    public Boolean containsIdentifier ( String identifier ) {
        Boolean found = false;
        for ( StringValue i : this )
            found = found || i.getValue().equals( identifier );
        return found;
    }
}
