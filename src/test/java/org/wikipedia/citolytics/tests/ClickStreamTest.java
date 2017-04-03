package org.wikipedia.citolytics.tests;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.clickstream.ClickStreamEvaluation;
import org.wikipedia.citolytics.clickstream.types.ClickStreamRecommendationResult;
import org.wikipedia.citolytics.clickstream.types.ClickStreamResult;
import org.wikipedia.citolytics.clickstream.utils.ValidateClickStreamData;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;

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



    @Before
    public void before() throws FileNotFoundException {
        wikiSimPath = resource("ClickStreamTest/wikisim_output.csv");
        wikiSimLangSimplePath = resource("ClickStreamTest/wikisim_output_lang_simple.csv");
        dataSetPath = resource("ClickStreamTest/clickstream.tsv");

        dataSetPath2 = resource("ClickStreamTest/clickstream_2.tsv");
        dataSetPathSimpleLang = resource("ClickStreamTest/clickstream_lang_simple.tsv");
        dataSetPathFormatTest = resource("ClickStreamTest/different_formats");
        langLinksPath = resource("ClickStreamTest/lang_links_enwiki.sql");

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
                .enableLocalEnvironment()
                .silent()
                .start(new String[]{
                        dataSetPath + "," + dataSetPath2,
                        "local"
                });

        assertEquals("Invalid number of records in data set", 44, job.output.size());
    }

    @Test
    public void testEvaluationSummary() throws Exception {
        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableLocalEnvironment().start("--wikisim " + wikiSimPath
                + " --gold " + dataSetPath
                + "," + dataSetPath2 // Multiple inputs
                + " --summary"
                + " --output local");

        System.out.println(job.output);

        assertEquals("Summary should have only a single result tuple", 1, job.output.size());
        assertEquals("Recommendations count invalid", 11, job.output.get(0).getRecommendationsCount());
        assertEquals("Impressions invalid", 129, job.output.get(0).getImpressions());
        assertEquals("Clicks invalid", 137, job.output.get(0).getTotalClicks());
    }

    @Ignore
    @Test
    public void testClickStreamEvaluation() throws Exception {
        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableLocalEnvironment().start("--wikisim " + wikiSimPath
                + " --gold " + dataSetPath
                + "," + dataSetPath2 // Multiple inputs
                + " --output local");

//        for(ClickStreamResult r: job.output) {
//            System.out.println(r);
//        }

        assertTrue("Needles not found", job.output.containsAll(getNeedles("")));
    }

    @Test
    public void testMultiLanguageClickStreamEvaluation() throws Exception {

        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableLocalEnvironment().enableLocalEnvironment().silent().start("--wikisim " + wikiSimLangSimplePath
                + " --gold " + dataSetPathSimpleLang
                + " --lang simple"
                + " --langlinks " + langLinksPath
                + " --output local");

        for(ClickStreamResult r: job.output) {
            System.out.println(r);
        }

        assertTrue("Needles not found", job.output.containsAll(getNeedles("simple_")));
    }

    @Test
    public void testDifferentFormatsAndMultiLang() throws Exception {
        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableLocalEnvironment().start("--wikisim " + wikiSimLangSimplePath
                + " --gold " + dataSetPathFormatTest
                + " --lang simple"
                + " --langlinks " + langLinksPath
                + " --id-title-mapping " + resource("ClickStreamTest/idtitle_mapping.in")
                + " --output local");

        System.out.println(job.output);
        assertTrue("Needles not found", job.output.containsAll(getNeedles("simple_")));
    }

    private ArrayList<ClickStreamResult> getNeedles(String langPrefix) {
        return new ArrayList<>(
                Arrays.asList(new ClickStreamResult[]{
                        new ClickStreamResult(
                                langPrefix + "QQQ",
                                new ArrayList<>(Arrays.asList(new ClickStreamRecommendationResult[]{
                                        new ClickStreamRecommendationResult(langPrefix + "CPA link", 3.0761422E7, 10),
                                        new ClickStreamRecommendationResult(langPrefix + "CPA nolink", 3.0761422E7, 20)
                                })),
                                2,
                                0,
                                38,
                                30,
                                30,
                                10
                        )
                })
        );
    }
}
