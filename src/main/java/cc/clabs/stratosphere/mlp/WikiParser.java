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

import cc.clabs.stratosphere.mlp.io.XMLChunkParser;
import cc.clabs.stratosphere.mlp.contracts.SentenceEmitter;

import eu.stratosphere.pact.common.contract.*;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactString;

public class WikiParser implements PlanAssembler, PlanAssemblerDescription {

    /**
    * {@inheritDoc}
    */
    public Plan getPlan( String... args ) {
        // parse job parameters
        String dataset = args[0];
        String output = args[1];
        String model = args[2];
        
        FileDataSource source = new FileDataSource( XMLChunkParser.class, dataset, "Input" );
        
        MapContract map = MapContract
                .builder( SentenceEmitter.class )
                .name( "Sentence Emitter" )
                .input( source )
                .build();
        map.setParameter( "MODEL", model );
        
        FileDataSink out = new FileDataSink( RecordOutputFormat.class, output, map, "Output" );
        RecordOutputFormat.configureRecordFormat( out )
                .recordDelimiter( '\n' )
                .fieldDelimiter( '\t' )
                .field(PactString.class, 0);
                
        Plan plan = new Plan( out, "Fancy Plan!" );
        plan.setDefaultParallelism( 1 );
        
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
