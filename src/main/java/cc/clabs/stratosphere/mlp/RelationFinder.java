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
import cc.clabs.stratosphere.mlp.types.*;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;

import eu.stratosphere.pact.common.contract.*;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class RelationFinder implements PlanAssembler, PlanAssemblerDescription {

    /**
    * {@inheritDoc}
    */
    @Override
    public Plan getPlan( String... args ) {
        // parse job parameters
        String dataset = args[0];
        String output = args[1];
        String model = args[2];
        
        String alpha = args[3];
        String beta  = args[4];
        String gamma = args[5];
        String threshold = args[6];
        
        Configuration conf = GlobalConfiguration.getConfiguration();
        conf.setInteger( "pact.parallelization.degree", -1 );
        conf.setInteger( "pact.parallelization.max-intra-node-degree", -1 );
        conf.setBoolean( "jobmanager.profiling.enable", true );
        
        FileDataSource source = new FileDataSource( WikiDocumentEmitter.class, dataset, "Dumps" );
        
        MapContract doc = MapContract
                .builder( DocumentProcessor.class )
                .name( "Processing Documents" )
                .input( source )
                .build();
        
        MapContract sentences = MapContract
                .builder( SentenceEmitter.class )
                .name( "Sentences" )
                .input( doc )
                .build();
        sentences.setParameter( "POS-MODEL", model );
        
        CoGroupContract candidates = CoGroupContract
                .builder( CandidateEmitter.class, PactInteger.class, 0, 0 )
                .name( "Candidates" )
                .input1( doc )
                .input2( sentences )
                .build();
        
        ReduceContract kernel = ReduceContract
                .builder( Analyzer.class, PactString.class, 0 )
                .name( "Kernel" )
                .input( candidates )
                .build();
        
        kernel.setParameter( "α", alpha );
        kernel.setParameter( "β", beta );
        kernel.setParameter( "γ", gamma );
        kernel.setParameter( "THRESHOLD", threshold );
        
        FileDataSink out = new FileDataSink( RecordOutputFormat.class, output, kernel, "Output" );
        RecordOutputFormat.configureRecordFormat( out )
                .recordDelimiter( '\n' )
                .fieldDelimiter( '\t' )
                .field( PactString.class, 0 )
                .field( PactDouble.class, 1 )
                .field( PactInteger.class, 2 )
                .field( PactWord.class, 3 )
                .field( PactSentence.class, 4 );
                
        Plan plan = new Plan( out, "Relation Finder" );
        
        return plan;
    }

    /**
    * {@inheritDoc}
    */
    @Override
    public String getDescription() {
        return "Parameters: [DATASET] [OUTPUT] [MODEL] [ALPHA] [BETA] [GAMMA] [THRESHOLD]";
    }
    
}
