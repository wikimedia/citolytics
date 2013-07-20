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

import java.io.StringWriter;
import java.util.LinkedList;
import java.util.regex.Pattern;
import org.eclipse.mylyn.wikitext.core.parser.Attributes;
import org.eclipse.mylyn.wikitext.core.parser.DocumentBuilder;

/**
 * A DocumentBuilder for the mylyn wikitext parser. It converts
 * a document written in MediaWiki-Markup into plaintext. Most
 * of the structure of the document will ne stripped, including
 * linebreaks, headings, etc.
 * 
 * @author rob
 */
public class PlaintextDocumentBuilder extends DocumentBuilder {
    
    private StringWriter writer = new StringWriter();
    private StringWriter stream = null;
    
    /**
     * These lists store all blocks within a block/span that will
     * not be rendered.
     */
    private LinkedList<BlockType> skipBlocks = new LinkedList<>();
    private LinkedList<SpanType> skipSpans = new LinkedList<>();
   
    
    public PlaintextDocumentBuilder( StringWriter stream ) {
        this.stream = stream;
    }

    @Override
    public void beginDocument() { }

    @Override
    public void endDocument() {
        String doc = StringUtils.unescapeEntities( writer.toString() );
                
        // remove remaining/undetected templates
        doc = Pattern.compile( "\\{\\{[^\\{]*?\\}\\}" ).matcher( doc ).replaceAll( "" );
        doc = Pattern.compile( "\\{\\{[^\\{]*?\\}\\}" ).matcher( doc ).replaceAll( "" );
        doc = Pattern.compile( "\\{\\{[^\\{]*?\\}\\}" ).matcher( doc ).replaceAll( "" );
        doc = Pattern.compile( "\\u2016[^\\u2016]*?\\u2016" ).matcher( doc ).replaceAll( "" );
        // remove dangling lines
        doc = Pattern.compile( "(:?\\A|\\n)\\s*[\\*\\|:].*" ).matcher( doc ).replaceAll( "" );
        doc = Pattern.compile( "\\}\\}\\s*" ).matcher( doc  ).replaceAll( "" );
        // remove undetected emphasis tags
        doc = Pattern.compile( "'{2,}" ).matcher( doc ).replaceAll( "" );
        // comments
        doc = Pattern.compile( "<!--.*?-->", Pattern.DOTALL ).matcher( doc ).replaceAll( "" );  
        // references
        doc = Pattern.compile( "<references>.*?</references>", Pattern.DOTALL ).matcher( doc ).replaceAll( "" );
        doc = Pattern.compile( "<ref[^>/]*>.*?</ref>", Pattern.DOTALL ).matcher( doc ).replaceAll( "" );
        doc = Pattern.compile( "<ref[^>]*>" ).matcher( doc ).replaceAll( "" );
        doc = Pattern.compile( "</ref[^>]*>" ).matcher( doc ).replaceAll( "" );
        // empty/unknown inline tags
        doc = Pattern.compile( "<([^ >]+)[^>]*>(.*?)</\\1>" ).matcher( doc ).replaceAll( "$2" );
        // non inline tags
        doc = Pattern.compile( "<([^ >]+)[^>]*/?>" ).matcher( doc ).replaceAll( " " );
        // fix for undetected links
        doc = Pattern.compile( "\\[\\[([^\\|]*)|([^\\]]*)]]" ).matcher( doc ).replaceAll( "$2" );
        doc = Pattern.compile( "\\[\\[[^\\[\\]]*]]" ).matcher( doc ).replaceAll( "" );
        // strip unneeded linebreaks, etc.
        doc = Pattern.compile( "\\n+" ).matcher( doc ).replaceAll( " " );
        doc = Pattern.compile( "\\s+" ).matcher( doc ).replaceAll( " " ); 
        // fix punctuation
        doc = Pattern.compile( "([\\w])[ ]+([:;,\\.\\)])", Pattern.DOTALL ).matcher( doc ).replaceAll( "$1$1" );
        // and strip parentheses from the plaintext
        //doc = Pattern.compile( "\\([^\\)]*\\)" ).matcher( doc ).replaceAll( "" );
        
        // good hackers trim!
        doc = doc.trim();
        
        stream.write( doc );
    }

    @Override
    public void beginBlock( BlockType type, Attributes attributes ) {
        switch ( type ) {
            // passing blocks
            case PARAGRAPH:
            case DEFINITION_ITEM:
            case DEFINITION_TERM:
            case NUMERIC_LIST:
            case DEFINITION_LIST:
            case BULLETED_LIST:
                if ( skipBlocks.size() > 0 ) skipBlocks.add( type );
                break;
            // block that will be skipped
            case TIP:
            case WARNING:
            case INFORMATION:
            case NOTE:
            case PANEL:
            case FOOTNOTE:
            case QUOTE:
            case CODE:
            case LIST_ITEM:
            case TABLE:
            case TABLE_ROW:
            case TABLE_CELL_HEADER:
            case TABLE_CELL_NORMAL:
            case PREFORMATTED:
                skipBlocks.add( type );
                break;
        }
    }

    @Override
    public void endBlock() {
        if ( !skipBlocks.isEmpty() )
            skipBlocks.removeLast();
        else
            writer.write( " " );
    }

    @Override
    public void beginSpan( SpanType type, Attributes attributes ) {
        switch ( type ) {
            // passing spans
            case EMPHASIS:
            case ITALIC:
            case SPAN:
            case STRONG:
            case SUBSCRIPT:
            case SUPERSCRIPT:
            case UNDERLINED:
            case CITATION:
                if ( skipSpans.size() > 0 ) skipSpans.add( type );
                break;
            // span that will be skipped
            case INSERTED:
            case DELETED:
            case MONOSPACE:
            case CODE:
                skipSpans.add( type );
                break;
        }
    }

    @Override
    public void endSpan() {
        if ( !skipSpans.isEmpty() )
            skipSpans.removeLast();
        else
            writer.write( " " );
    }

    @Override
    public void beginHeading( int level, Attributes attributes ) {
        skipSpans.add( SpanType.SPAN );
    }

    @Override
    public void endHeading() {
        if ( !skipSpans.isEmpty() )
            skipSpans.removeLast();
    }

    @Override
    public void characters( String text ) {
        if ( skipBlocks.size() > 0 ) return;
        if ( skipSpans.size() > 0 ) return;
        writer.write( text );
    }

    @Override
    public void entityReference( String entity ) {
        writer.write( '&'+ entity + ';' );
    }

    @Override
    public void image( Attributes attributes, String url ) { }

    @Override
    public void link( Attributes attributes, String link, String text ) {
        // nothing to do
        if ( link.isEmpty() && text.isEmpty() ) return;
        String full = (link + text).toLowerCase();
        // skip
        if ( skipBlocks.size() > 0 ) return;
        if ( skipSpans.size() > 0 ) return;
        // special link types
        if ( full.contains( "category:" ) ) return;
        if ( full.contains( "image:" ) ) return;
        if ( full.contains( "file:" ) ) return;
        if ( full.contains( "thumb" ) ) return;
        if ( full.contains( "|" ) ) return;
        // urls, beacause the parse also detects raw links
        if ( Pattern.compile( "https?:" ).matcher( full ).matches() ) return;
        // language links
        if ( Pattern.compile( "\\w{2}:" ).matcher( text ).matches() ) return;
        
        
        // when textfield is emtpy the link will be shown, except
        // anything in parentheses.
        if ( text.isEmpty() ) text = link.replaceAll( "\\(.*?\\)", "" );
        writer.write( '"' + text + '"' );
    }

    @Override
    public void imageLink( Attributes linkAttributes, Attributes imageAttributes, String href, String imageUrl ) { }

    @Override
    public void acronym( String text, String definition ) {
        writer.write( text );
    }

    @Override
    public void lineBreak() {
        writer.write( " " );
    }

    @Override
    public void charactersUnescaped( String literal ) {
        writer.write( literal );
    }
    
}
