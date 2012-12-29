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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import uk.ac.ed.ph.snuggletex.SnuggleEngine;
import uk.ac.ed.ph.snuggletex.SnuggleInput;
import uk.ac.ed.ph.snuggletex.SnuggleSession;

/**
 * 
 * @author rob
 */
public class TexIdentifierExtractor {
    
    /**
     * list of false positive identifiers
     */
    private final static List<String> blacklist = Arrays.asList(
        "sin", "cos", "min", "max", "inf", "lim", "log", "exp",
        "sup", "lim sup", "lim inf", "arg", "dim", "cosh",
        "⋯", ":", "'", "′", "…", "∞", "Λ", "⋮", " ", " ", "~",
        ";", "#"
    );
    
    /**
     * Returns a list of all identifers within a given formula.
     * The formula is coded in TeX.
     * 
     * @param formula TeX representation of a formula
     * @return list of identifiers
     */
    public static ArrayList<String> getAll( String formula ) {
        // create vanilla SnuggleEngine and new SnuggleSession
        SnuggleEngine engine = new SnuggleEngine();
        SnuggleSession session = engine.createSession();
        try {
            SnuggleInput input = new SnuggleInput( "$$ "+ cleanupTexString( formula ) +" $$" );
            session.parseInput( input );
            String xml = session.buildXMLString();
            return getIdentifiersFrom( xml );
        }
        catch ( Exception e ) {
            return new ArrayList<String>();
        }
    }
    
    /**
     * Returns a list of unique identifiers from a MathML string.
     * This function searches for all <mi/> or <ci/> tags within
     * the string.
     * 
     * @param mathml 
     * @return a list of unique identifiers. When no identifiers were
     *         found, an empty list will be returned.
     */
    private static ArrayList<String> getIdentifiersFrom( String mathml ) {
        ArrayList<String> list = new ArrayList<String>();
        Pattern p = Pattern.compile( "<([mc]i)>(.*?)</\\1>", Pattern.DOTALL );
        Matcher m = p.matcher( mathml );
        while ( m.find() ) {
            String identifier = m.group( 2 );
            if ( blacklist.contains( identifier ) ) continue;
            if ( list.contains( identifier ) ) continue;
            list.add( identifier );
        }
        return list;
    }
    
    /**
     * Returns a cleaned version of the TeX string.
     * 
     * @param tex the TeX string
     * @return the cleaned TeX string
     */
    private static String cleanupTexString( String tex ) {
        // strip text blocks
        tex = tex.replaceAll( "\\\\text\\{.*?\\}", "" );
        return tex;
    } 
    
}
