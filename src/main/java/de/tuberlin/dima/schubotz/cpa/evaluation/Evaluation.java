package de.tuberlin.dima.schubotz.cpa.evaluation;

import de.tuberlin.dima.schubotz.cpa.evaluation.io.LinksResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.MLTResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.SeeAlsoResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.WikiSimResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.EvaluationOuterJoin;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.EvaluationReducer;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.LinkExistsFilter;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

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
    public static String seeAlsoDelimiter = "#";

    public static void main(String[] args) throws Exception {

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println("Usage: " + getDescription());
            System.exit(1);
        }


        String outputFilename = args[0];
        String seeAlsoInputFilename = args[1];
        String wikiSimInputFilename = args[2];
        String mltInputFilename = args[3];
        String linksInputFilename = args[4];

        boolean enableLinkFilter = true;

        if (linksInputFilename.equals("nofilter")) {
            enableLinkFilter = false;
        }

        //final int MIN_MATCHES_COUNT = (args.length > 3 ? Integer.valueOf(args[3]) : 1);

        // Sorted DESC
        final int[] firstN = new int[]{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}; //, 10, 15, 20};

        final boolean aggregate = (args.length > 5 && args[5].equals("y") ? true : false);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // Prepare CPA: Existing links, Project to CPA + CoCit

        DataSet<LinkResult> links = env.readFile(new LinksResultInputFormat(), linksInputFilename);
//        DataSet<Long> linkHashes = links.map(new MapFunction<LinkResult, Long>() {
//            @Override
//            public Long map(LinkResult in) throws Exception {
//                return StringUtils.hash((String) in.getField(0) + (String) in.getField(1));
//            }
//        });

//        linkHashes.print();

        // Filter existing links
        DataSet<WikiSimPlainResult> wikiSimResults = env.readFile(new WikiSimResultInputFormat(), wikiSimInputFilename);

        if (enableLinkFilter) {
            wikiSimResults = wikiSimResults
                    .coGroup(links)
                    .where(1)
                    .equalTo(0)
                    .with(new LinkExistsFilter<WikiSimPlainResult>(1, 2));
        }


        // CPA
        DataSet<EvaluationResult> cpaResults = wikiSimResults
                .project(WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.CPA_KEY)
                .types(String.class, String.class, Double.class)
                .union(
                        wikiSimResults
                                .project(WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.CPA_KEY)
                                .types(String.class, String.class, Double.class)
                )
                .groupBy(0)

                .sortGroup(2, Order.DESCENDING)
                .sortGroup(1, Order.ASCENDING)

                .first(firstN[0])
                .groupBy(0)
                .reduceGroup(new EvaluationReducer<Tuple3<String, String, Double>>(firstN[0]));


        // CoCit
        DataSet<EvaluationResult> cocitResults = wikiSimResults
//        DataSet<Tuple3<String, String, Integer>> cocitResults = wikiSimResults
                .project(WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.COCIT_KEY)
                .types(String.class, String.class, Integer.class)
                .union(
                        wikiSimResults
                                .project(WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.COCIT_KEY)
                                .types(String.class, String.class, Integer.class)
                )
                .groupBy(0)
                .sortGroup(2, Order.DESCENDING)
                .sortGroup(1, Order.ASCENDING) // Order chain required for consistent results

                .first(firstN[0])
                .groupBy(0)
                .reduceGroup(new EvaluationReducer<Tuple3<String, String, Integer>>(firstN[0]));

        // Prepare MLT
        DataSet<MLTResult> mltTmpResults = env.readFile(new MLTResultInputFormat(), mltInputFilename);

        if (enableLinkFilter) {
            mltTmpResults = mltTmpResults
                    // Filter existing links
                    .coGroup(links)
                    .where(0)
                    .equalTo(0)
                    .with(new LinkExistsFilter<MLTResult>(0, 1));
        }

        DataSet<EvaluationResult> mltResults = mltTmpResults
                .groupBy(0)
                .sortGroup(2, Order.DESCENDING)
                .sortGroup(1, Order.ASCENDING)
                .first(firstN[0])
                .groupBy(0)
                .reduceGroup(new EvaluationReducer<MLTResult>(firstN[0]));

        // Prepare SeeAlso
        DataSet<EvaluationFinalResult> seeAlsoResults = env.readFile(new SeeAlsoResultInputFormat(), seeAlsoInputFilename)
                // possible filter: if total = X
                .map(new MapFunction<SeeAlsoResult, EvaluationFinalResult>() {
                    @Override
                    public EvaluationFinalResult map(SeeAlsoResult in) throws Exception {
                        String[] list = ((String) in.getField(1)).split(seeAlsoDelimiter);
                        return new EvaluationFinalResult(
                                (String) in.getField(0), list);

                    }
                });

        // Outer Join SeeAlso x CPA
        DataSet<EvaluationFinalResult> output = seeAlsoResults
                .coGroup(cpaResults)
                .where(0)
                .equalTo(0)
                .with(new EvaluationOuterJoin(firstN, EvaluationFinalResult.CPA_LIST_KEY, EvaluationFinalResult.CPA_MATCHES_KEY))

                        // CoCIt
                .coGroup(cocitResults)
                .where(0)
                .equalTo(0)
                .with(new EvaluationOuterJoin(firstN, EvaluationFinalResult.COCIT_LIST_KEY, EvaluationFinalResult.COCIT_MATCHES_KEY))

                        // MLT
                .coGroup(mltResults)
                .where(0)
                .equalTo(0)
                .with(new EvaluationOuterJoin(firstN, EvaluationFinalResult.MLT_LIST_KEY, EvaluationFinalResult.MLT_MATCHES_KEY));

//        if(filter) {
//           output = output.filter(new FilterFunction<EvaluationFinalResult>() {
//                    @Override
//                    public boolean filter(EvaluationFinalResult record) throws Exception {
//                        if(
//                                (int) record.getField(record.MLT_MATCHES_KEY) > 0
//                                && (int) record.getField(record.CPA_MATCHES_KEY) > 0
//                                && (int) record.getField(record.COCIT_MATCHES_KEY) > 0
//                            ) {
//                            return true;
//                        } else {
//                            return false;
//                        }
//                    }
//                });
//        }

        if (aggregate) {
            // Aggregate
            output = output.reduce(new ReduceFunction<EvaluationFinalResult>() {
                @Override
                public EvaluationFinalResult reduce(EvaluationFinalResult first, EvaluationFinalResult second) throws Exception {

                    EvaluationFinalResult res = new EvaluationFinalResult("total", EvaluationFinalResult.EMPTY_LIST);

                    for (int i = 0; i < firstN.length; i++) {
                        res.aggregateField(first, second, EvaluationFinalResult.CPA_MATCHES_KEY + i);
                        res.aggregateField(first, second, EvaluationFinalResult.COCIT_MATCHES_KEY + i);
                        res.aggregateField(first, second, EvaluationFinalResult.MLT_MATCHES_KEY + i);
                    }

                    return res;
                }

            });
        }

        // Make arrays printable
        DataSet<EvaluationResultOutput> outputNice = output.map(new MapFunction<EvaluationFinalResult, EvaluationResultOutput>() {
            @Override
            public EvaluationResultOutput map(EvaluationFinalResult in) throws Exception {
                return new EvaluationResultOutput(in);
            }
        });

        if (outputFilename.equals("print")) {
            outputNice.print();
        } else {
            outputNice.writeAsCsv(outputFilename, csvRowDelimiter, String.valueOf(csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }


        env.execute("Evaluation");
    }


    public static String getDescription() {
        return "OUTPUT SEEALSO WIKISIM MLT LINKS [AGGREGATE]";
    }


}
