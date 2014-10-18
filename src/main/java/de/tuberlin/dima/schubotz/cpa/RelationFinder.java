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
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.distributions.UniformIntegerDistribution;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;

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

        // Mapper
        /*
            DocumentProcessor.map
                doc new WikiDoc ( ...)
                Collector out
                doc.collectLinks(out)
                    loop all outlinks
                        out.collect( Record: linkTuple, distance, count )
         */
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

        //CsvOutputFormat test = new CsvOutputFormat();
        //test.setWriteMode( FileSystem.WriteMode.OVERWRITE );
        //FileDataSink out = new FileDataSink( test, output, filter, "Output" );


        FileDataSink out = new FileDataSink( CsvOutputFormat.class, output, filter, "Output" );
        CsvOutputFormat.configureRecordFormat( out )

                .recordDelimiter('\n')
                .fieldDelimiter(';')
                .field(LinkTuple.class, 0)
                .field(IntValue.class, 1)
                .field(IntValue.class, 2)
                .field(LongValue.class, 3)
                .field(DoubleValue.class, 4)
                .field(IntValue.class, 5)
                .field(IntValue.class, 6)
        ;

        UniformIntegerDistribution distribution = new UniformIntegerDistribution(Integer.parseInt(threshold), 2000);
        //out.setGlobalOrder(countOrder, distribution);

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
