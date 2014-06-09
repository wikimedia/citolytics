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
package de.tuberlin.dima.schubotz.cpa;

import de.tuberlin.dima.schubotz.cpa.contracts.DocumentProcessor;
import de.tuberlin.dima.schubotz.cpa.contracts.calculateCPA;
import de.tuberlin.dima.schubotz.cpa.io.WikiDocumentEmitter;
import de.tuberlin.dima.schubotz.cpa.types.LinkTuple;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
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
        String dataSet = args[0];
        String output = args[1];

        String alpha = ((args.length > 2) ? args[2] : "1.5");
        String threshold = ((args.length > 3) ? args[3] : "1");

        FileDataSource source = new FileDataSource(WikiDocumentEmitter.class, dataSet, "Dumps");

        MapOperator doc = MapOperator
                .builder( DocumentProcessor.class )
                .name( "Processing Documents" )
                .input( source )
                .build();


        ReduceOperator filter = ReduceOperator
                .builder(calculateCPA.class, LinkTuple.class, 0)
                .name("Filter")
                .input(doc)
                .build();

        Ordering countOrder = new Ordering(2, IntValue.class, Order.DESCENDING);
        filter.setGroupOrder(countOrder);

        filter.setParameter("THRESHOLD", threshold);
        filter.setParameter("α", alpha);

        /* 0 target.addField(linkTuple);
        1 target.addField(distance);
        2 target.addField(count);
        3 distSquared
        4 recDistα
        5 min
        6 max */
        FileDataSink out = new FileDataSink( CsvOutputFormat.class, output, filter, "Output" );
        CsvOutputFormat.configureRecordFormat( out )
                .recordDelimiter('\n')
                .fieldDelimiter(';')
                .field(LinkTuple.class, 0)
                .field(DoubleValue.class, 1)
                .field(IntValue.class, 2)
                .field(IntValue.class, 3)
                .field(DoubleValue.class, 4)
                .field(IntValue.class, 5)
                .field(IntValue.class, 6)
        ;
        out.setGlobalOrder(countOrder);

        return new Plan(out, "CPA-demo");
    }

    /**
    * {@inheritDoc}
    */
    @Override
    public String getDescription() {
        return "Parameters: [DATASET] [OUTPUT] [ALPHA] [THRESHOLD]";
    }
    
}
