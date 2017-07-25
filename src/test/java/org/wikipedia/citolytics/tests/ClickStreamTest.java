package org.wikipedia.citolytics.tests;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.clickstream.ClickStreamEvaluation;
import org.wikipedia.citolytics.clickstream.ClickStreamStats;
import org.wikipedia.citolytics.clickstream.ClickStreamTranslator;
import org.wikipedia.citolytics.clickstream.types.ClickStreamRecommendationResult;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.clickstream.utils.ValidateClickStreamData;
import org.wikipedia.citolytics.tests.utils.Tester;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IllegalFormatConversionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClickStreamTest extends Tester {
    private String wikiSimPath;
    private String wikiSimLangSimplePath;
    private String dataSetPath;
    private String dataSetPath2;
    private String dataSetPathFormatTest;

    private String dataSetPathSimpleLang;
    private String langLinksPath;
    private String articleStatsPath;



    @Before
    public void before() throws FileNotFoundException {
        wikiSimPath = resource("ClickStreamTest/wikisim_output.csv");
        wikiSimLangSimplePath = resource("ClickStreamTest/wikisim_output_lang_simple.csv");
        dataSetPath = resource("ClickStreamTest/clickstream.tsv");

        dataSetPath2 = resource("ClickStreamTest/clickstream_2.tsv");
        dataSetPathSimpleLang = resource("ClickStreamTest/clickstream_lang_simple.tsv");
        dataSetPathFormatTest = resource("ClickStreamTest/different_formats");
        langLinksPath = resource("ClickStreamTest/lang_links_enwiki.sql");
        articleStatsPath = resource("ClickStreamTest/stats.in");
    }

    @Ignore
    @Test
    public void validateLocalDataSet() throws Exception {
        ValidateClickStreamData.main(
                new String[]{
                        dataSetPath,
                        "print"
                });
    }

    @Test
    public void validateDataSet() throws Exception {
        ValidateClickStreamData job = new ValidateClickStreamData();

        job
                .enableTestEnvironment()
                .start(new String[]{
                        dataSetPath + "," + dataSetPath2,
                        "local"
                });

        assertEquals("Invalid number of records in data set", 44, job.output.size());
    }

    @Test
    public void testEvaluationSummary() throws Exception {
        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableTestEnvironment().start("--wikisim " + wikiSimPath
                + " --gold " + dataSetPath
                + "," + dataSetPath2 // Multiple inputs
                + " --summary"
                + " --output local --page-id-a -1 --page-id-b -1");

//        System.out.println(job.output);

        assertEquals("Summary should have only a single result tuple", 1, job.output.size());
//        assertEquals("Recommendations count invalid", 11, job.output.get(0).getRecommendationsCount());
        assertEquals("Recommendations count invalid", 9, job.output.get(0).getRecommendationsCount());

        assertEquals("Impressions invalid", 129, job.output.get(0).getImpressions());
        assertEquals("Clicks invalid", 137, job.output.get(0).getTotalClicks());
        assertEquals("Optimal clicks invalid", 137, job.output.get(0).getOptimalClicks());
    }

    @Test
    public void testEvaluationSummaryTopK() throws Exception {
        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableTestEnvironment().start("--wikisim " + wikiSimPath
                + " --gold " + dataSetPath
                + "," + dataSetPath2 // Multiple inputs
                + " --summary --topk 1"
                + " --output local --page-id-a -1 --page-id-b -1");

        assertEquals("Summary should have only a single result tuple", 1, job.output.size());
        assertEquals("Recommendations count invalid", 5, job.output.get(0).getRecommendationsCount());
        assertEquals("Impressions invalid", 129, job.output.get(0).getImpressions());
        assertEquals("Total clicks invalid", 137, job.output.get(0).getTotalClicks());
        assertEquals("Optimal clicks invalid", 119, job.output.get(0).getOptimalClicks());
    }


    @Test
    public void testEvaluationSummaryWithTopRecommendations() throws Exception {
        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableTestEnvironment().start("--wikisim " + wikiSimPath
                + " --gold " + dataSetPath
                + "," + dataSetPath2 // Multiple inputs
                + " --summary"
                + " --top-recommendations " + resource("top_recommendations.out", true)
                + " --output local --page-id-a -1 --page-id-b -1");

//        System.out.println(job.output);

        assertEquals("Summary should have only a single result tuple", 1, job.output.size());
//        assertEquals("Recommendations count invalid", 11, job.output.get(0).getRecommendationsCount());
        assertEquals("Recommendations count invalid", 9, job.output.get(0).getRecommendationsCount());

        assertEquals("Impressions invalid", 129, job.output.get(0).getImpressions());
        assertEquals("Clicks invalid", 137, job.output.get(0).getTotalClicks());
        assertEquals("Optimal clicks invalid", 137, job.output.get(0).getOptimalClicks());

    }

    @Test
    public void testClickStreamEvaluation() throws Exception {
        setJob(new ClickStreamEvaluation()).start("--wikisim " + wikiSimPath
                + " --gold " + dataSetPath
                + "," + dataSetPath2 // Multiple inputs
                + " --output local --page-id-a -1 --page-id-b -1");

//        System.out.println(getJobOutputAsString(job));

        assertEquals("Invalid number of results returned", 5, job.getOutput().size());
        assertTrue("Needles not found", job.getOutput().containsAll(getNeedles("")));
    }

    @Test
    public void testClickStreamEvaluationTopK() throws Exception {
        setJob(new ClickStreamEvaluation()).start("--wikisim " + wikiSimPath
                + " --gold " + dataSetPath
                + "," + dataSetPath2 // Multiple inputs
                + " --topk 1 --output local --page-id-a -1 --page-id-b -1");

//        System.out.println(getJobOutputAsString(job));

        assertEquals("Invalid number of results returned", 5, job.getOutput().size());
        // TODO Fixture instead of fixture: assertTrue("Needles not found", job.getOutput().containsAll(getNeedles("")));
    }

    @Test
    public void testClickStreamStats() throws Exception {
        setJob(new ClickStreamStats())
                .start("--input " + dataSetPath + " --output local");

        assertEquals("Invalid result count", 1, job.output.size());
    }


    @Test
    public void testMultiLanguageClickStreamEvaluation() throws Exception {

        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableTestEnvironment().start("--wikisim " + wikiSimLangSimplePath
                + " --gold " + dataSetPathSimpleLang
                + " --lang simple"
                + " --langlinks " + langLinksPath
                + " --output local --page-id-a -1 --page-id-b -1");

//        System.out.println(getJobOutputAsString(job));

        assertTrue("Needles not found", job.output.containsAll(getNeedles("simple_")));
    }

    @Test
    public void testClickStreamTranslator() throws Exception {

        ClickStreamTranslator job = new ClickStreamTranslator();

        job.enableTestEnvironment().start("--input " + dataSetPathSimpleLang
                + " --lang simple"
                + " --langlinks " + langLinksPath
                + " --output local");

//        System.out.println(getJobOutputAsString(job));
        assertEquals("Invalid number of results", 4, job.getOutput().size());
//        assertTrue("Needles not found", job.output.containsAll(getNeedles("simple_")));
    }



    @Test
    public void testDifferentFormatsAndMultiLang() throws Exception {
        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableTestEnvironment().start("--wikisim " + wikiSimLangSimplePath
                + " --gold " + dataSetPathFormatTest
                + " --lang simple"
                + " --langlinks " + langLinksPath
                + " --id-title-mapping " + resource("ClickStreamTest/idtitle_mapping.in")
                + " --output local --page-id-a -1 --page-id-b -1");

//        System.out.println(job.output);
        assertTrue("Needles not found", job.output.containsAll(getNeedles("simple_")));
    }

    @Ignore
    @Test
    public void testClickStreamWithCPI() throws Exception {
        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableTestEnvironment().start("--wikisim " + wikiSimPath
                + " --gold " + dataSetPath + "," + dataSetPath2 // Multiple inputs
                + " --cpi x*log(z/(y+1)) --article-stats " + articleStatsPath
                + " --output print --page-id-a -1 --page-id-b -1");
    }

    @Test
    public void testMathExpressionCPI() throws Exception {

        double score = 0.5;
        int inLinks = 10;
        int articleCount = 1000;
        String cpiExpr = "%1$f*Math.log(%3$d/%2$d)";

        try {
            ScriptEngineManager mgr = new ScriptEngineManager();
            ScriptEngine engine = mgr.getEngineByName("JavaScript");

            double cpi = (double) engine.eval(String.format(cpiExpr, score, inLinks, articleCount));
            assertEquals("Invalid CPI score", 2.302585092994046, cpi, 0);

        } catch(ScriptException | IllegalFormatConversionException e) {
            throw new Exception("Cannot evaluate CPI script expression: " + cpiExpr + "; Exception: " + e.getMessage());
        }
    }

    private ArrayList<ClickStreamResult> getNeedles(String langPrefix) {
        return new ArrayList<>(
                Arrays.asList(new ClickStreamResult[]{
                        new ClickStreamResult(
                                langPrefix + "QQQ",
                                new ArrayList<>(Arrays.asList(new ClickStreamRecommendationResult[]{
                                        new ClickStreamRecommendationResult(langPrefix + "CPA link", 1314.0, 10),
                                        new ClickStreamRecommendationResult(langPrefix + "CPA nolink", 1314.0, 20)
                                })),
                                2,
                                0,
                                38,
                                30,
                                30,
                                10,
                                38
                        )
                })
        );
    }
}
