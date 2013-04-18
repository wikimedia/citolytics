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
        // equals number of cores per node
        //conf.setInteger( "pact.parallelization.max-intra-node-degree", 2 );
        //conf.setBoolean( "jobmanager.profiling.enable", true );
        //conf.setInteger( "pact.parallelization.degree", -1 );

        
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
        // order sentences by their position within the document
        candidates.setGroupOrderForInputTwo( new Ordering( 2, PactDouble.class, Order.ASCENDING ) );
        // set the weighting factors
        candidates.setParameter( "α", alpha );
        candidates.setParameter( "β", beta );
        candidates.setParameter( "γ", gamma );

        
        ReduceContract filter = ReduceContract
                .builder( FilterCandidates.class, PactInteger.class, 0 )
                .name( "Filter" )
                .input( candidates )
                .build();
        // order candidates by the identifier
        filter.setGroupOrder( new Ordering( 1, PactRelation.class, Order.ASCENDING ) );
        // sets the minimum threshold for a candidate's score
        filter.setParameter( "THRESHOLD", threshold );

        
        
        FileDataSink out = new FileDataSink( RecordOutputFormat.class, output, filter, "Output" );
        RecordOutputFormat.configureRecordFormat( out )
                .recordDelimiter( '\n' )
                .fieldDelimiter( '\t' )
                .field( PactRelation.class, 0 );
                
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
