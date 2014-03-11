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
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.CoGroupOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;

public class RelationFinder implements Program, ProgramDescription {

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
        
        //Configuration conf = GlobalConfiguration.getConfiguration();
        // equals number of cores per node
        //conf.setInteger( "pact.parallelization.max-intra-node-degree", 2 );
        //conf.setBoolean( "jobmanager.profiling.enable", true );
        //conf.setInteger( "pact.parallelization.degree", -1 );

        
        FileDataSource source = new FileDataSource( WikiDocumentEmitter.class, dataset, "Dumps" );
        
        MapOperator doc = MapOperator
                .builder( DocumentProcessor.class )
                .name( "Processing Documents" )
                .input( source )
                .build();

        MapOperator sentences = MapOperator
                .builder( SentenceEmitter.class )
                .name( "Sentences" )
                .input( doc )
                .build();
        sentences.setParameter( "POS-MODEL", model );
        
        CoGroupOperator candidates = CoGroupOperator
                .builder( CandidateEmitter.class, IntValue.class, 0, 0 )
                .name( "Candidates" )
                .input1( doc )
                .input2( sentences )
                .build();
        // order sentences by their position within the document
        candidates.setGroupOrderForInputTwo( new Ordering( 2, DoubleValue.class, Order.ASCENDING ) );
        // set the weighting factors
        candidates.setParameter( "α", alpha );
        candidates.setParameter( "β", beta );
        candidates.setParameter( "γ", gamma );


        ReduceOperator filter = ReduceOperator
                .builder(FilterCandidates.class, IntValue.class, 0)
                .name( "Filter" )
                .input( candidates )
                .build();
        // order candidates by the identifier
        filter.setGroupOrder( new Ordering( 1, PactRelation.class, Order.ASCENDING ) );
        // sets the minimum threshold for a candidate's score
        filter.setParameter( "THRESHOLD", threshold );


        FileDataSink out = new FileDataSink( CsvOutputFormat.class, output, filter, "Output" );
        CsvOutputFormat.configureRecordFormat( out )
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
