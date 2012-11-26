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
package cc.clabs.stratosphere.mlp.contracts;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import eu.stratosphere.nephele.configuration.Configuration;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author rob
 */
@ConstantFields(fields={})
@OutCardBounds(lowerBound=0, upperBound=OutCardBounds.UNBOUNDED)
public class SentenceEmitter extends MapStub {
    
    MaxentTagger tagger = null;
    
    private static final Log LOG = LogFactory.getLog( SentenceEmitter.class );
    
    // initialize reusable mutable objects
    private final PactRecord output = new PactRecord();
    private final PactString string = new PactString();

    @Override
    public void open(Configuration parameter) throws Exception {
      super.open( parameter );
      tagger = new MaxentTagger( parameter.getString( "MODEL", "models/wsj-0-18-left3words-distsim.tagger") );
    }
    
    @Override
    public void map( PactRecord record, Collector<PactRecord> collector ) {
        String corpus = ((PactString) record.getField( 0, PactString.class ) ).getValue();
        
        corpus = stripMarkup( corpus );
        
        List<List<HasWord>> sentences = MaxentTagger.tokenizeText( new StringReader( corpus ) );
        for (List<HasWord> sentence : sentences) {
            ArrayList<TaggedWord> tSentence = tagger.tagSentence(sentence);
            string.setValue( Sentence.listToString(tSentence, false) );
            output.setField( 0, string );
            collector.collect( output );
        }
    }
    
    private String stripMarkup( String corpus ) {
        // replace links
        corpus = Pattern.compile( "\\[\\[([^|]+?)]]", Pattern.DOTALL).matcher( corpus ).replaceAll( "$1" );
        corpus = Pattern.compile( "\\[\\[[^]]+\\|(.+?)]]", Pattern.DOTALL).matcher( corpus ).replaceAll( "$1" );
        // tables
        corpus = Pattern.compile( "\\{\\|.*?\\|\\}", Pattern.DOTALL).matcher( corpus ).replaceAll( "" );
        // templates/variables
        corpus = Pattern.compile( "\\{\\{.*?\\}\\}", Pattern.DOTALL).matcher( corpus ).replaceAll( "" );
        // headlines
        corpus = Pattern.compile( "(=+).*?\\1", Pattern.DOTALL).matcher( corpus ).replaceAll( "" );
        // Files/Images
        corpus = Pattern.compile( "\\[\\[(Image|File|Category):.+?]]", Pattern.DOTALL ).matcher( corpus ).replaceAll( "" );
        // references
        corpus = Pattern.compile( "<ref.*?></ref>", Pattern.DOTALL ).matcher( corpus ).replaceAll( "" );
        corpus = Pattern.compile( "<ref.*?/>", Pattern.DOTALL ).matcher( corpus ).replaceAll( "" );
        // comments
        corpus = Pattern.compile( "<!--.*?-->", Pattern.DOTALL ).matcher( corpus ).replaceAll( "" );
        // html specials
        corpus = corpus.replace( "&lt;", "<" );
        corpus = corpus.replace( "&le;", "\u2264" );
        corpus = corpus.replace( "&ge;", "\u2266" );
        corpus = corpus.replace( "&ne;", "\u2260" );
        corpus = corpus.replace( "&gt;", ">" );
        corpus = corpus.replace( "&quot;", "\"" );
        corpus = corpus.replace( "&nbsp;", " " );
        corpus = corpus.replace( "&ensp;", " " );
        corpus = corpus.replace( "&emsp;", " " );
        corpus = corpus.replace( "&thinsp;", " " );
        corpus = corpus.replace( "&minus;", "\u2212" );
        corpus = corpus.replace( "&plusmn;", "\u00b1" );
        corpus = corpus.replace( "&times;", "\u00d7" );
        corpus = corpus.replace( "&sdot;", "\u22c5" );
        corpus = corpus.replace( "&ndash;", "-" );
        corpus = corpus.replace( "&mdash;", "-" );
        corpus = corpus.replace( "&amp;", "&" );
        return corpus;
    }
    
}
