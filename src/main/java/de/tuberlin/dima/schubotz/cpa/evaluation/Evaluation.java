package de.tuberlin.dima.schubotz.cpa.evaluation;

import de.tuberlin.dima.schubotz.cpa.evaluation.io.LinksResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.MLTResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.SeeAlsoResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.io.WikiSimResultInputFormat;
import de.tuberlin.dima.schubotz.cpa.evaluation.operators.*;
import de.tuberlin.dima.schubotz.cpa.evaluation.types.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.regex.Pattern;

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

    public String outputFilename;
    public String seeAlsoInputFilename;
    public String wikiSimInputFilename;
    public String mltInputFilename;
    public String linksInputFilename;

    public int wikiSimCpaKey;
    public int wikiSimCoCitKey;
    public int[] topKs;

    public boolean enableLinkFilter = false;
    public boolean enableCPA = true;
    public boolean enableMLT = false;
    public boolean enableCoCit = false;


    private DataSet<EvaluationResult> evaluationResults;
    private DataSet<WikiSimPlainResult> wikiSimResults;
    private DataSet<LinkResult> links;

    private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    public Evaluation() {

    }

    public static void main(String[] args) throws Exception {

        if (args.length <= 1) {
            System.err.println("Input/output parameters missing!");
            System.err.println("Usage: " + getDescription());
            System.exit(1);
        }

        Evaluation job = new Evaluation();

        job.outputFilename = args[0];
        job.seeAlsoInputFilename = args[1];
        job.wikiSimInputFilename = args[2];
        job.mltInputFilename = args[3];
        job.linksInputFilename = args[4];


        // Sorted DESC
        job.topKs = (args.length > 5 ? parseTopKsInput(args[5]) : new int[]{10}); //, 10, 15, 20};
        job.wikiSimCpaKey = (args.length > 6 ? Integer.parseInt(args[6]) : WikiSimPlainResult.CPA_KEY);
        job.wikiSimCoCitKey = (args.length > 6 ? Integer.parseInt(args[6]) : WikiSimPlainResult.CPA_KEY);

//        final boolean aggregate = (args.length > 7 && args[7].equals("y") ? true : false);

        EvaluationResult.topKlength = job.topKs.length;

        job.execute();
    }

    public DataSet<LinkResult> getLinksDataSet() {
        if (links == null) {
            links = env.readFile(new LinksResultInputFormat(), linksInputFilename);
        }
        return links;
    }

    public void execute() throws Exception {
        if (linksInputFilename.toLowerCase().equals("nofilter")) {
            enableLinkFilter = false;
        }

        // Filter existing links
        // setIncludeFields <--- wikiSimCpaKey
        wikiSimResults = env.readFile(new WikiSimResultInputFormat().setCPAKey(wikiSimCpaKey), wikiSimInputFilename);

        if (enableLinkFilter) {
            wikiSimResults = wikiSimResults
                    .coGroup(getLinksDataSet())
                    .where(1)
                    .equalTo(0)
                    .with(new LinkExistsFilter<WikiSimPlainResult>(1, 2));
        }

        //Outer Join SeeAlso x CPA
        evaluationResults = getSeeAlsoDataSet();

        evaluateCPA();
        evaluateCoCit();
        evaluateMLT();

        DataSet<EvaluationResult> output = evaluationResults;

        if (outputFilename.equals("print")) {
            output.print();
        } else {
            output.writeAsCsv(outputFilename, csvRowDelimiter, String.valueOf(csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("Evaluation");
    }

    public DataSet<EvaluationResult> getSeeAlsoDataSet() {
        int parallelism = 10;

        return env.readFile(new SeeAlsoResultInputFormat(), seeAlsoInputFilename)
                .setParallelism(parallelism)
                        // possible filter: if total = X
                .map(new MapFunction<SeeAlsoResult, EvaluationResult>() {
                    @Override
                    public EvaluationResult map(SeeAlsoResult in) throws Exception {
                        String[] list = ((String) in.getField(1)).split(seeAlsoDelimiter);
                        return new EvaluationResult(
                                (String) in.getField(0), list);

                    }
                })
                .setParallelism(parallelism);
    }


    public void evaluateCPA() {
        if (!enableCPA)
            return;

        DataSource<String> lines = env.readTextFile(wikiSimInputFilename);

        Configuration config = new Configuration();
        config.setInteger("scoreField", wikiSimCpaKey);

        // Input
        DataSet<Tuple3<String, String, Double>> cpaDataSet = lines.flatMap(new RichFlatMapFunction<String, Tuple3<String, String, Double>>() {
            protected int page1_field = 1;
            protected int page2_field = 2;
            protected int score_field;
            protected String fieldDelimitter = "|";

            @Override
            public void open(Configuration parameter) throws Exception {
                super.open(parameter);

                score_field = parameter.getInteger("scoreField", 8);
            }

            @Override
            public void flatMap(String line, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String[] fields = line.split(Pattern.quote(fieldDelimitter));

//                out.collect(new ResultRecord<>(
//                        fields[page1_field], fields[page2_field], Double.valueOf(fields[score_field])
//                ));
//                out.collect(new ResultRecord<>(
//                        fields[page2_field], fields[page1_field], Double.valueOf(fields[score_field])
//                ));

                out.collect(new Tuple3<>(fields[page1_field], fields[page2_field], Double.valueOf(fields[score_field])));
                out.collect(new Tuple3<>(fields[page2_field], fields[page1_field], Double.valueOf(fields[score_field])));

            }
        }).withParameters(config);

        // Prepare
//        DataSet<ListResult> cpaResults = cpaDataSet
//                .groupBy(0)
//                .reduceGroup(new ListBuilder<Double>(topKs[0]));
//                .setParallelism();

        // Evaluate
        evaluationResults = evaluationResults

                .coGroup(cpaDataSet)
//                .coGroup(cpaResults)
                .where(0)
                .equalTo(0)
                .with(new MatchesCounterPlainResults(topKs,
                        EvaluationResult.CPA_LIST_KEY,
                        EvaluationResult.CPA_MATCHES_KEY,
                        EvaluationResult.CPA_HRR_KEY,
                        EvaluationResult.CPA_TOPK_KEY));
    }

    public void evaluateCoCit() {
        if (!enableCoCit)
            return;

        // Prepare
        DataSet<Tuple3<String, String, Integer>> cocitResultsA = wikiSimResults
                .project(WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.COCIT_KEY);

        DataSet<Tuple3<String, String, Integer>> cocitResultsB = wikiSimResults
                .project(WikiSimPlainResult.PAGE2_KEY, WikiSimPlainResult.PAGE1_KEY, WikiSimPlainResult.COCIT_KEY);


        DataSet<ListResult> cocitResults = cocitResultsA
                .union(cocitResultsB)
                .map(new ListMapper<Integer>())
                .groupBy(0)
                .reduceGroup(new ListBuilder<Integer>(topKs[0]));

        // Evaluate
        evaluationResults = evaluationResults
                .coGroup(cocitResults)
                .where(0)
                .equalTo(0)
                .with(new MatchesCounter(topKs,
                        EvaluationResult.COCIT_LIST_KEY,
                        EvaluationResult.COCIT_MATCHES_KEY,
                        EvaluationResult.COCIT_HRR_KEY,
                        EvaluationResult.COCIT_TOPK_KEY));
    }

    public void evaluateMLT() {
        if (!enableMLT)
            return;

        // Prepare MLT
        DataSet<Tuple3<String, String, Float>> mltTmpResults = env.readFile(new MLTResultInputFormat(), mltInputFilename)
                .project(0, 1, 2);

        if (enableLinkFilter) {
            mltTmpResults = mltTmpResults
                    // Filter existing links
                    .coGroup(getLinksDataSet())
                    .where(0)
                    .equalTo(0)
                    .with(new LinkExistsFilter<Tuple3<String, String, Float>>(0, 1));
        }

        DataSet<ListResult> mltResults = mltTmpResults
                .map(new ListMapper<Float>())
                .groupBy(0)
                .reduceGroup(new ListBuilder<Float>(topKs[0]));


        evaluationResults = evaluationResults
                .coGroup(mltResults)
                .where(0)
                .equalTo(0)
                .with(new MatchesCounter(topKs,
                        EvaluationResult.MLT_LIST_KEY,
                        EvaluationResult.MLT_MATCHES_KEY,
                        EvaluationResult.MLT_HRR_KEY,
                        EvaluationResult.MLT_TOPK_KEY))
        ;
    }

    public static String getDescription() {
        return "[OUTPUT] [SEEALSO-DATASET] [WIKISIM-DATASET] [MLT-DATASET] [LINKS/NOFILTER] [TOP-K1, TOP-K2, ...] [CPA-FIELD] [COCIT-FIELD]";
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


    public void filterResults() {
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
    }

    public void aggregateResults() {
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
    }
}
