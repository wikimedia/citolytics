package de.tuberlin.dima.schubotz.cpa.evaluation;

import de.tuberlin.dima.schubotz.cpa.WikiSim;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.*;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.EvaluationReducer;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.OuterJoin;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.*;
import de.tuberlin.dima.schubotz.cpa.types.LinkTuple;
import de.tuberlin.dima.schubotz.cpa.types.StringListValue;
import de.tuberlin.dima.schubotz.cpa.types.WikiSimResult;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Evaluation
 * -> Count matches from result links and see also links
 * <p/>
 * Article | SeeAlso Links   | CoCit Links | CoCit Matches | CPA Links | CPA Matches |  MLT Links | MLT Matches
 * ---
 * Page1   | Page2, 3, 6, 7  |  3, 9       | 1             | 12, 3, 7  |    2        |  2, 3      | 2
 * ---
 * Sum ...
 * <p/>
 * ***********
 * <p/>
 * - Article
 * - SeeAlso Links (List String)
 * -- matches (int)
 * - CPA Links
 * -- matches
 * - CoCit Links
 * -- matches
 * - MLT Links
 * -- matches
 */
public class Evaluation {
    public static String csvRowDelimiter = "\n";
    public static char csvFieldDelimiter = '|';


    public static void main(String[] args) throws Exception {

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println(new WikiSim().getDescription());
            System.exit(1);
        }


        String outputFilename = args[0];
        String seeAlsoInputFilename = args[1];
        String wikiSimInputFilename = args[2];
        String mltInputFilename = args[3];
        String linksInputFilename = args[4];


        //final int MIN_MATCHES_COUNT = (args.length > 3 ? Integer.valueOf(args[3]) : 1);
        final int firstN = (args.length > 5 ? Integer.valueOf(args[5]) : 10);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // Prepare CPA: Existing links, Project to CPA + CoCit

        DataSet<LinkResult> links = env.readFile(new LinksResultInputFormat(), linksInputFilename);
        DataSet<Long> linkHashes = links.map(new MapFunction<LinkResult, Long>() {
            @Override
            public Long map(LinkResult in) throws Exception {
                return LinkTuple.hash((String) in.getField(0) + (String) in.getField(1));
            }
        });

//        linkHashes.print();

        // Filter existing links
        DataSet<WikiSimPlainResult> wikiSimResults = env.readFile(new WikiSimResultInputFormat(), wikiSimInputFilename)
                .filter(new RichFilterFunction<WikiSimPlainResult>() {
                    Collection<Long> linkHashes;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        linkHashes = getRuntimeContext().getBroadcastVariable("linkHashes");
                    }

                    @Override
                    public boolean filter(WikiSimPlainResult in) throws Exception {
                        // Filter if link exists (link hash exists)
                        if (linkHashes.contains(LinkTuple.hash((String) in.getField(1) + (String) in.getField(2)))
                                || linkHashes.contains(LinkTuple.hash((String) in.getField(2) + (String) in.getField(1)))
                                ) {
//                        if (linkHashes.contains(in.getField(0))) {
                            return false;
                        } else {
                            return true;
                        }
                    }
                }).withBroadcastSet(linkHashes, "linkHashes");


        // CPA
        DataSet<EvaluationResult> cpaResults = wikiSimResults
                .project(WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.CPA_KEY)
                .types(String.class, String.class, Double.class)
                .groupBy(0)
                .sortGroup(2, Order.DESCENDING)

                .first(firstN)
                .groupBy(0)
                .reduceGroup(new EvaluationReducer<Tuple3<String, String, Double>>());


        // CoCit
        DataSet<EvaluationResult> cocitResults = wikiSimResults
                .project(WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.COCIT_KEY)
                .types(String.class, String.class, Long.class)
                .groupBy(0)
                .sortGroup(2, Order.DESCENDING)

                .first(firstN)
                .groupBy(0)
                .reduceGroup(new EvaluationReducer<Tuple3<String, String, Long>>());


        // Prepare MLT
        DataSet<EvaluationResult> mltResults = env.readFile(new MLTResultInputFormat(), mltInputFilename).groupBy(0)
                .sortGroup(2, Order.DESCENDING)
                .first(firstN)
                .groupBy(0)
                .reduceGroup(new EvaluationReducer<MLTResult>());

        // Prepare SeeAlso
        DataSet<EvaluationResult> seeAlsoResults = env.readFile(new SeeAlsoResultInputFormat(), seeAlsoInputFilename)
                .map(new MapFunction<SeeAlsoResult, EvaluationResult>() {
                    @Override
                    public EvaluationResult map(SeeAlsoResult in) throws Exception {
                        String[] list = ((String) in.getField(1)).split(",");
                        return new EvaluationResult((String) in.getField(0), StringListValue.valueOf(list));
                    }
                });

        // Outer Join SeeAlso x CPA
        DataSet<EvaluationFinalResult> output = seeAlsoResults
                .coGroup(cpaResults)
                .where(0)
                .equalTo(0)
                .with(new CoGroupFunction<EvaluationResult, EvaluationResult, EvaluationFinalResult>() {
                    @Override
                    public void coGroup(Iterable<EvaluationResult> first, Iterable<EvaluationResult> second, Collector<EvaluationFinalResult> out) throws Exception {

                        Iterator<EvaluationResult> iterator1 = first.iterator();
                        Iterator<EvaluationResult> iterator2 = second.iterator();

                        EvaluationResult record1 = null;
                        EvaluationResult record2 = null;


                        if (iterator1.hasNext()) {
                            record1 = iterator1.next();
                            StringListValue list1 = (StringListValue) record1.getField(1);

                            EvaluationFinalResult outRecord = new EvaluationFinalResult(
                                    (String) record1.getField(0), list1);

                            if (iterator2.hasNext()) {
                                record2 = iterator2.next();
                                StringListValue list2 = (StringListValue) record2.getField(1);

                                outRecord.setField(list2, 2);
                                outRecord.setField(ListUtils.intersection(list1, list2).size(), 3);

                            }

                            out.collect(outRecord);
                        }
                    }
                })
                        // CoCIt
                .coGroup(cocitResults)
                .where(0)
                .equalTo(0)
                .with(new OuterJoin(EvaluationFinalResult.COCIT_LIST_KEY))
                        // MLT
                .coGroup(mltResults)
                .where(0)
                .equalTo(0)
                .with(new OuterJoin(EvaluationFinalResult.MLT_LIST_KEY));

        if (outputFilename.equals("print")) {
            output.print();
        } else {
            output.writeAsCsv(outputFilename, csvRowDelimiter, String.valueOf(csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("Evaluation");
    }
}
