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
package cc.clabs.stratosphere.mlp;

import cc.clabs.stratosphere.mlp.contracts.*;
import cc.clabs.stratosphere.mlp.io.*;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;

import eu.stratosphere.pact.common.contract.*;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class WikiParser implements PlanAssembler, PlanAssemblerDescription {

    /**
    * {@inheritDoc}
    */
    @Override
    public Plan getPlan( String... args ) {
        // parse job parameters
        String dataset = args[0];
        String output = args[1];
        String model = args[2];
        
        Configuration conf = GlobalConfiguration.getConfiguration();
        conf.setInteger( "pact.parallelization.degree", -1 );
        conf.setInteger( "pact.parallelization.max-intra-node-degree", -1 );
        conf.setBoolean( "jobmanager.profiling.enable", true );
        
        FileDataSource source = new FileDataSource( WikiDumpParser.class, dataset, "Dumps" );
        
        MapContract doc = MapContract
                .builder( WikiDocumentEmitter.class )
                .name( "WikiDocuments" )
                .input( source )
                .build();
        
        MapContract sentences = MapContract
                .builder( SentenceEmitter.class )
                .name( "Sentences" )
                .input( doc )
                .build();
        sentences.setParameter( "POS-MODEL", model );
        
        CoGroupContract tagger = CoGroupContract
                .builder( SentenceTagger.class, PactInteger.class, 0, 0 )
                .name( "Tagger" )
                .input1( doc )
                .input2( sentences )
                .build();
        
        FileDataSink out = new FileDataSink( RecordOutputFormat.class, output, tagger, "Output" );
        RecordOutputFormat.configureRecordFormat( out )
                .recordDelimiter( '\n' )
                .fieldDelimiter( '\t' )
                .field(PactString.class, 0)
                .field(PactString.class, 1);
                
        Plan plan = new Plan( out, "WikiParser" );
        
        return plan;
    }

    /**
    * {@inheritDoc}
    */
    @Override
    public String getDescription() {
        return "Parameters: [input] [output]";
    }
    
}
