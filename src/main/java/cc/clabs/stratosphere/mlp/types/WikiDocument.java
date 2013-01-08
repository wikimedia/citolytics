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

import cc.clabs.stratosphere.mlp.utils.PlaintextDocumentBuilder;
import cc.clabs.stratosphere.mlp.utils.SerializationUtils;
import cc.clabs.stratosphere.mlp.utils.StringUtils;
import cc.clabs.stratosphere.mlp.utils.TexIdentifierExtractor;

import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.eclipse.mylyn.wikitext.core.parser.MarkupParser;
import org.eclipse.mylyn.wikitext.core.parser.markup.MarkupLanguage;
import org.eclipse.mylyn.wikitext.mediawiki.core.MediaWikiLanguage;

/**
 * @author rob
 */
public class WikiDocument implements Value {
    
    private static final Log LOG = LogFactory.getLog( WikiDocument.class );

    
    /*
     * Raw text of the document
     */
    private String text = null;
    
    /*
     * Plaintext version of the document
     */
    private String plaintext = null;
    
    /*
     * Title of the document
     */
    private String title = null;
    
    /*
     * Wikipedia id of the document
     */
    private Integer id = null;
    
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
    private Integer ns = null;
    
    /*
     * Holds all formulas found within the document. The key of
     * the HashMap is the replacement string in the document and
     * the value contains the TeX String
     */
    private HashMap<String,String> formulas = new HashMap<>();
    
    /*
     * Stores all unique identifiers found in this document
     */
    private ArrayList<String> knownIdentifiers = new ArrayList<>();
    
    /**
     * Returns a plaintext version of this document.
     * 
     * @return a plaintext string
     */
    public String getPlainText() {
        StringWriter writer = new StringWriter();
        MarkupParser parser = new MarkupParser();
        MarkupLanguage wiki = new MediaWikiLanguage();
        parser.setMarkupLanguage( wiki );
        parser.setBuilder( new PlaintextDocumentBuilder( writer ) );
        parser.parse( text );
        plaintext = writer.toString();
        return plaintext;
    }

    @Override
    public void write( DataOutput out ) throws IOException {
        out.writeInt( id );
        out.writeInt( ns );
        SerializationUtils.writeString( out, title );
        SerializationUtils.writeString( out, text );
        SerializationUtils.writeString( out, plaintext );
        out.writeInt( formulas.size() );
        for ( Entry<String,String> entry : formulas.entrySet() ) {
            SerializationUtils.writeString( out, entry.getKey() );
            SerializationUtils.writeString( out, entry.getValue() );
        }
        out.write( knownIdentifiers.size() );
        for ( String identifier : knownIdentifiers ) {
            SerializationUtils.writeString( out, identifier );
        }
    }

    @Override
    public void read( DataInput in ) throws IOException {
        id = in.readInt();
        ns = in.readInt();
        title = SerializationUtils.readNextString( in );
        text = SerializationUtils.readNextString( in );
        plaintext = SerializationUtils.readNextString( in );
        // math tags
        for ( int i = in.readInt(); i > 0 ; i-- ) {
            String key = SerializationUtils.readNextString( in );
            String value = SerializationUtils.readNextString( in );
            formulas.put( key, value );
        }
        // identifiers
        for ( int i = in.readInt(); i > 0 ; i-- ) {
            knownIdentifiers.add( SerializationUtils.readNextString( in ) );
        }
    }
    
    /**
     * Returns the document id.
     * 
     * @return id of the document
     */
    public int getId() {
        return id;
    }
    
    /**
     * Returns the document title.
     * 
     * @return title of the document
     */
    public String getTitle() {
        return title;
    }
    
    
    /**
     * Sets the id of the document
     * @param id
     */
    public void setId( int id ) {
        this.id = id;
    }

    /**
     * Sets the title of the document
     * @param title 
     */
    public void setTitle( String title ) {
        this.title = title;
    }

    /**
     * Returns the namespace id of the document.
     * 
     * @return namespace id
     */
    public int getNS() {
        return ns;
    }

    /**
     * Sets the namespace of the document.
     * 
     * @param ns 
     */
    public void setNS( int ns ) {
        this.ns = ns;
    }

    /**
     * Returns the raw text body of the document.
     * 
     * @return the text body
     */
    public String getText() {
        return text;
    }
    
    /**
     * Sets the text body of the document.
     * 
     * @param text 
     */
    public void setText( String text ) {
        this.text = StringUtils.unescapeEntities( text );
        this.replaceMathTags();
    }
    
    
    /**
     * Helper that replaces all math tags from the document
     * and stores them in a list. Math tags that contain exactly
     * on identifier will be replaced inline with the identifier.
     */
    private void replaceMathTags() {
        Pattern p = Pattern.compile( "<math>(.*?)</math>", Pattern.DOTALL );
        Matcher m;
        String key, formula;
        while ( (m = p.matcher( text )).find() ) {
            
            key = " MATH" + Long.toHexString( (long) (Math.random() * 0x3b9aca00) ).toUpperCase() + " ";
            formula = m.group( 1 ).trim();
            
            
            ArrayList<String> identifiers = TexIdentifierExtractor.getAll( formula );
            if ( identifiers.isEmpty() ) {
                text = m.replaceFirst( "" );
            }
            else if ( identifiers.size() == 1 ) {
                text = m.replaceFirst( identifiers.get( 0 ) );
            }
            else {
                formulas.put( key, formula );
                text = m.replaceFirst( key );
            }
                
            
            // add found identifers to the page wide list
            for ( String identifier : identifiers ) {
                if ( knownIdentifiers.contains( identifier ) ) continue;
                knownIdentifiers.add( identifier );
            }            
            
        }
    }
    
    /**
     * Returns the hashmap of all found and replaced formulas.
     * 
     * @return a hashmap of all formulars
     */
    public HashMap<String,String> getFormulas() {
        return formulas;
    }

    /**
     * Returns a list of all found unique identifiers within
     * this document.
     * 
     * @return a list of unique identifiers
     */
    public ArrayList<String> getKnownIdentifiers() {
        return knownIdentifiers;
    }
    
}
