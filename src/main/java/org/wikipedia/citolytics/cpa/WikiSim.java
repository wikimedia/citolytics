package org.wikipedia.citolytics.cpa;

import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cirrussearch.IdTitleMappingExtractor;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.operators.CPAReducer;
import org.wikipedia.citolytics.cpa.operators.MissingIdRemover;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;
import org.wikipedia.citolytics.cpa.types.WikiSimResult;
import org.wikipedia.citolytics.redirects.RedirectExtractor;
import org.wikipedia.citolytics.redirects.operators.ReplaceRedirectsWithOuterJoin;
import org.wikipedia.citolytics.redirects.single.WikiSimRedirects;
import org.wikipedia.processing.DocumentProcessor;

/**
 * Flink job for computing CPA results depended on CPI alpha values.
 *
 *
 * Arguments:
 * --input  [path]  Wikipedia XML Dump (hdfs)
 * --output [path]  Output filename (hdfs)
 * --alpha  [double,...]   CPI alpha values, each value is represented by a separate column in the output (comma separated; e.g. 0.5,1.0,1.5)
 * --reducer-threshold  [int]    Reducer threshold: Discard records with lower number of co-citations (default: 1)
 * --combiner-threshold [int]   Combiner threshold: Discard records with lower number of co-citations (default: 1)
 * --format [str]   Format of Wikipedia XML Dump (default: 2013; set to "2006" for older dumps)
 * --redirects [path]  Resolve redirects? Set path to redirects set (default: n)
 * --resolve-redirects
 * --remove-missing-ids
 * --keep-infobox
 */
public class WikiSim extends WikiSimAbstractJob<WikiSimResult> {

    private Configuration config;
    public String inputFilename;
    public String redirectsFilename;
    public String alpha = "1.5";
    public boolean removeMissingIds = false;
    public boolean resolveRedirects = false;
    private boolean median = true;
    private boolean wiki2006 = false;
    private boolean removeInfoBox = false;
    private int reducerThreshold = 1;
    private int combinerThreshold = 1;

    private DataSource<String> wikiDump;

    public static String getUsage() {
        return "Usage: --input [DATASET] --output [OUTPUT] --alpha [ALPHA1, ALPHA2, ...] " +
                "--reducer-threshold [REDUCER-THRESHOLD] --combiner-threshold [COMBINER-THRESHOLD] --format [WIKI-VERSION] " +
                "--redirects [REDIRECTS-DATESET]";
    }

    /**
     * Executes Flink job
     *
     * @param args {input, output, alpha, reducerThreshold, combinerThreshold, wiki-format, redirects}
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new WikiSim().start(args);
    }

    public void init() {
        jobName = "WikiSim";

        // Bug fix (heartbeat bug)
        // https://issues.apache.org/jira/browse/FLINK-2299

        Configuration clusterConfig = new Configuration();
        clusterConfig.setString(ConfigConstants.AKKA_TRANSPORT_HEARTBEAT_PAUSE, "600s");
        GlobalConfiguration.includeConfiguration(clusterConfig);

        // ---

        ParameterTool params = ParameterTool.fromArgs(args);

        if (args.length <= 1) {
            System.err.println("Error: --input and --output parameters missing!");
            System.err.println(WikiSim.getUsage());
            System.exit(1);
        }

        // Read arguments
        config = null;
        inputFilename = params.get("input");
        outputFilename = params.get("output");
        redirectsFilename = params.get("redirects");

        alpha = params.get("alpha", "1.5");
        reducerThreshold = params.getInt("reducer-threshold", 1);
        combinerThreshold = params.getInt("combiner-threshold", 1);
        wiki2006 = params.get("format", "2013").equalsIgnoreCase("2006");
        removeInfoBox = !params.has("keep-infobox");
        removeMissingIds = params.has("remove-missing-ids");
        resolveRedirects = params.has("resolve-redirects");
    }

    /**
     * Returns current job configuration
     *
     * @return Job configuration
     */
    public Configuration getConfig() {
        if(config == null) {
            // Configuration for 2nd order functions
            config = new Configuration();
            config.setString("alpha", alpha);
            config.setInteger("reducerThreshold", reducerThreshold);
            config.setInteger("combinerThreshold", combinerThreshold);

            config.setBoolean("median", true);
            config.setBoolean("wiki2006", wiki2006);
            config.setBoolean("removeInfoBox", removeInfoBox);
        }
        return config;
    }

    public void plan() throws Exception {

        // Read Wikipedia XML Dump
        wikiDump = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);

        // Compute CPA recommendations
        result = wikiDump.flatMap(new DocumentProcessor()) // TODO alpha in flatMap
                .withParameters(getConfig())
                .groupBy(WikiSimResult.HASH_KEY)
                .reduce(new CPAReducer())
                .setCombineHint(ReduceOperatorBase.CombineHint.HASH)
                .withParameters(getConfig());


        // Resolve redirects if requested
        if (redirectsFilename != null || resolveRedirects) {
            jobName += " + redirects";
            result = resolveRedirects(env, result, redirectsFilename);
        }

        // Remove results without ids, i.e. do not exist as page
        if (removeMissingIds) {
            jobName += " + id removal";
            result = MissingIdRemover.removeMissingIds(result, IdTitleMappingExtractor.extractIdTitleMapping(env, wikiDump));
        }
    }

    /**
     * Resolve Wikipedia redirects in results
     * <p/>
     * Wikipedia uses inconsistent internal links, therefore, we need to check each result record
     * for redirects, map redirects to their targets and sum up resulting duplicates.
     *
     * @param env ExecutionEnvironment
     * @param wikiSimResults Result tuples with redirects
     * @param pathToRedirects Path to redirects CSV (HDFS or local)
     * @return Result set with resolved redirects
     */
    public DataSet<WikiSimResult> resolveRedirects(ExecutionEnvironment env, DataSet<WikiSimResult> wikiSimResults, String pathToRedirects) {
        DataSet<RedirectMapping> redirects;

        // fields
        int hash = WikiSimResult.HASH_KEY;
        int pageA = WikiSimResult.PAGE_A_KEY;
        int pageB = WikiSimResult.PAGE_A_KEY;
        int redirectSource = 0;

        if(pathToRedirects == null) {
            // Load redirects from XML dump
            redirects = RedirectExtractor.extractRedirectMappings(env, wikiDump);
        } else {
            // Load redirects from pre-processed data set
            redirects = WikiSimRedirects.getRedirectsDataSet(env, pathToRedirects);
        }

        return wikiSimResults
                // replace page names with redirect target
                // page A
                .leftOuterJoin(redirects)
                .where(pageA)
                .equalTo(redirectSource)
                .with(new ReplaceRedirectsWithOuterJoin(pageA))
                // page B
                .leftOuterJoin(redirects)
                .where(pageB)
                .equalTo(redirectSource)
                .with(new ReplaceRedirectsWithOuterJoin(pageB))
                // sum duplicated tuples
                .groupBy(hash)
                .reduce(new CPAReducer())
                .setCombineHint(ReduceOperatorBase.CombineHint.HASH);
    }



}
