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

import eu.stratosphere.pact.common.type.Value;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.text.translate.AggregateTranslator;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.commons.lang3.text.translate.EntityArrays;
import org.apache.commons.lang3.text.translate.LookupTranslator;
import org.apache.commons.lang3.text.translate.NumericEntityUnescaper;
import org.apache.commons.lang3.text.translate.UnicodeEscaper;
import org.apache.commons.lang3.text.translate.UnicodeUnescaper;

/**
 * @author rob
 */
public class WikiPage implements Value {
    
    public String text = null;
    
    public String title = null;
    
    public Integer id = null;
    
    /**
     * Wikipedia pages belong to different namespaces. Below
     * is a list that describes a commonly used namespaces.
     * 
     *  -2	Media
     *  -1	Special
     *  0	Default
     *  1	Talk
     *  2	User
     *  3	User talk
     *  4	Wikipedia
     *  5	Wikipedia talk
     *  6	File
     *  7	File talk
     *  8	MediaWiki
     *  9	MediaWiki talk
     *  10	Template
     *  11	Template talk
     *  12	Help
     *  13	Help talk
     *  14	Category
     *  15	Category talk
     *  100	Portal
     *  101	Portal talk
     *  108	Book
     *  109	Book talk
     */
    public Integer ns = null;
    
    public String getPlainText() {
        String body = this.text;
        // strip htmlesque blocks except math
        body = Pattern.compile( "<([^math >]+)[^>]*>[^<]*</\\1>|<[^math\\s/>]+[^/>]*/{0,1}>" , Pattern.DOTALL ).matcher( body ).replaceAll( "" );
        // comments
        body = Pattern.compile( "<!--.*?-->", Pattern.DOTALL ).matcher( body ).replaceAll( "" );        
        // replace links
        body = Pattern.compile( "\\[\\[(?:Image|File|Category):[^\\]]+?]]", Pattern.DOTALL ).matcher( body ).replaceAll( "" ); 
        body = Pattern.compile( "\\[\\[([^\\|:]+?)]]", Pattern.DOTALL ).matcher( body ).replaceAll( "''$1''" );
        body = Pattern.compile( "\\[\\[[^\\|]+\\|([^\\]]+?)]]", Pattern.DOTALL ).matcher( body ).replaceAll( "''$1''" );
        // templates/variables
        //body = Pattern.compile( "\\{\\{.+?\\}\\}", Pattern.DOTALL ).matcher( body ).replaceAll( "" );
        // tables
        //body = Pattern.compile( "\\{\\|.*?\\|\\}", Pattern.DOTALL).matcher( body ).replaceAll( "" );
        // headlines
        body = Pattern.compile( "(=+)[^=]+?\\1", Pattern.DOTALL ).matcher( body ).replaceAll( "" );
        // strip unneeded linebreaks
        body = Pattern.compile( "\\n+", Pattern.DOTALL ).matcher( body ).replaceAll( "\n" );
     
        return body;
    }

    @Override
    public void write( DataOutput out ) throws IOException {
        out.writeInt( id );
        out.writeInt( ns );
        out.writeInt( title.length() );
        out.writeUTF( title );
        out.writeInt( text.length() );
        out.writeUTF( text );
    }

    @Override
    public void read( DataInput in ) throws IOException {
        id = in.readInt();
        ns = in.readInt();
        int len = in.readInt();
        byte[] buffer = new byte[ len ];
        // title
        in.readFully( buffer );
        title = buffer.toString();
        // text
        len = in.readInt();
        buffer = new byte[ len ];
        in.readFully( buffer );
        text = buffer.toString();
    }
    
    
}
