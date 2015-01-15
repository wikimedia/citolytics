package de.tuberlin.dima.schubotz.cpa.evaluation;

import de.tuberlin.dima.schubotz.cpa.evaluation.io.LinksResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.MLTResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.SeeAlsoResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.WikiSimResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.LinkExistsFilter;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.ListBuilder;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.MatchesCounter;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.*;
import org.apache.flink.api.common.functions.MapFunction;
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

        boolean enableLinkFilter = false;

        if (linksInputFilename.toLowerCase().equals("nofilter")) {
            enableLinkFilter = false;
        }

        // Sorted DESC
        final int[] topKs = (args.length > 5 ? parseTopKsInput(args[5]) : new int[]{10}); //, 10, 15, 20};
        int wikiSimCpaKey = (args.length > 6 ? Integer.parseInt(args[6]) : WikiSimPlainResult.CPA_KEY);
        final boolean aggregate = (args.length > 7 && args[7].equals("y") ? true : false);

        MatchesCounter.topKs = topKs;

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
        // setIncludeFields <--- wikiSimCpaKey
        DataSet<WikiSimPlainResult> wikiSimResults = env.readFile(new WikiSimResultInputFormat().setCPAKey(wikiSimCpaKey), wikiSimInputFilename);

        if (enableLinkFilter) {
            wikiSimResults = wikiSimResults
                    .coGroup(links)
                    .where(1)
                    .equalTo(0)
                    .with(new LinkExistsFilter<WikiSimPlainResult>(1, 2));
        }

        // CPA
        DataSet<ListResult> cpaResults = wikiSimResults
                .project(WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.CPA_KEY)
                .types(String.class, String.class, Double.class)
                .union(
                        wikiSimResults
                                .project(WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.CPA_KEY)
                                .types(String.class, String.class, Double.class)
                )
                .groupBy(0)
                .reduceGroup(new ListBuilder<Double, Tuple3<String, String, Double>>(topKs[0]));


        // CoCit
        DataSet<ListResult> cocitResults = wikiSimResults
                .project(WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.COCIT_KEY)
                .types(String.class, String.class, Integer.class)
                .union(
                        wikiSimResults
                                .project(WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.COCIT_KEY)
                                .types(String.class, String.class, Integer.class)
                )
                .groupBy(0)
                .reduceGroup(new ListBuilder<Double, Tuple3<String, String, Integer>>(topKs[0]));


        // Prepare MLT
        DataSet<MLTResult> mltTmpResults = env.readFile(new MLTResultInputFormat(), mltInputFilename);

//        if (enableLinkFilter) {
//            mltTmpResults = mltTmpResults
//                    // Filter existing links
//                    .coGroup(links)
//                    .where(0)
//                    .equalTo(0)
//                    .with(new LinkExistsFilter<MLTResult>(0, 1));
//        }

        DataSet<ListResult> mltResults = mltTmpResults
                .groupBy(0)
                .reduceGroup(new ListBuilder<Float, MLTResult>(topKs[0]));

        // Prepare SeeAlso
        DataSet<EvaluationResult> seeAlsoResults = env.readFile(new SeeAlsoResultInputFormat(), seeAlsoInputFilename)
                // possible filter: if total = X
                .map(new MapFunction<SeeAlsoResult, EvaluationResult>() {
                    @Override
                    public EvaluationResult map(SeeAlsoResult in) throws Exception {
                        String[] list = ((String) in.getField(1)).split(seeAlsoDelimiter);
                        return new EvaluationResult(
                                (String) in.getField(0), list);

                    }
                });

        //Outer Join SeeAlso x CPA
        DataSet<EvaluationResult> output = seeAlsoResults
                .coGroup(cpaResults)
                .where(0)
                .equalTo(0)
                .with(new MatchesCounter(topKs, EvaluationResult.CPA_LIST_KEY, EvaluationResult.CPA_MATCHES_KEY))

                        // CoCIt
                .coGroup(cocitResults)
                .where(0)
                .equalTo(0)
                .with(new MatchesCounter(topKs, EvaluationResult.COCIT_LIST_KEY, EvaluationResult.COCIT_MATCHES_KEY))

                        // MLT
                .coGroup(mltResults)
                .where(0)
                .equalTo(0)
                .with(new MatchesCounter(topKs, EvaluationResult.MLT_LIST_KEY, EvaluationResult.MLT_MATCHES_KEY));

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

//        if (aggregate) {
//            // Aggregate
//            output = output.reduce(new ReduceFunction<EvaluationResult>() {
//                @Override
//                public EvaluationResult reduce(EvaluationResult first, EvaluationResult second) throws Exception {
//
//                    EvaluationResult res = new EvaluationResult("total", EvaluationResult.EMPTY_LIST);
//
//                    for (int i = 0; i < topKs.length; i++) {
//                        res.aggregateField(first, second, EvaluationResult.CPA_MATCHES_KEY + i);
//                        res.aggregateField(first, second, EvaluationResult.COCIT_MATCHES_KEY + i);
//                        res.aggregateField(first, second, EvaluationResult.MLT_MATCHES_KEY + i);
//                    }
//
//                    return res;
//                }
//
//            });
//        }

        if (outputFilename.equals("print")) {
            output.print();
        } else {
            output.writeAsCsv(outputFilename, csvRowDelimiter, String.valueOf(csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("Evaluation");
    }

    public static String getDescription() {
        return "OUTPUT SEEALSO-DATASET WIKISIM-DATASET MLT-DATASET LINKS/NOFILTER [TOP-K1, TOP-K2, ...] CPA-FIELD";
    }


    public static int[] parseTopKsInput(String str) {
        String[] array = str.split(",");
        int[] output = new int[array.length];
        for (int i = 0; i < array.length; i++) {
            output[i] = Integer.parseInt(array[i]);

            if (i > 0 && output[i - 1] <= output[i]) {
                System.err.println("TopKs is not ordered descending. " + output[i - 1] + " <= " + output[i]);
                System.exit(1);
            }
        }
        return output;
    }
}
