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
    private String dataSetPathSimpleLang;
    private String langLinksPath;



    @Before
    public void before() throws FileNotFoundException {
        wikiSimPath = resource("ClickStreamTest/wikisim_output.csv");
        wikiSimLangSimplePath = resource("ClickStreamTest/wikisim_output_lang_simple.csv");
        dataSetPath = resource("ClickStreamTest/clickstream.tsv");

        dataSetPath2 = resource("ClickStreamTest/clickstream_2.tsv");
        dataSetPathSimpleLang = resource("ClickStreamTest/clickstream_lang_simple.tsv");
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

        job.start("--wikisim " + wikiSimPath
                + " --gold " + dataSetPath
                + "," + dataSetPath2 // Multiple inputs
                + " --summary"
                + " --output local");

        assertEquals("Summary should have only a single result tuple", 1, job.output.size());
        assertEquals("Recommendation count invalid", 2, job.output.get(0).getRecommendations().size());
        assertEquals("Impressions invalid", 129, job.output.get(0).getImpressions());
        assertEquals("Clicks invalid", 137, job.output.get(0).getTotalClicks());
    }

    @Ignore
    @Test
    public void testClickStreamEvaluation() throws Exception {
        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.start("--wikisim " + wikiSimPath
                + " --gold " + dataSetPath
                + "," + dataSetPath2 // Multiple inputs
                + " --output local");

        // Needles
        ArrayList<ClickStreamResult> needles = new ArrayList<>(
            Arrays.asList(new ClickStreamResult[]{
                new ClickStreamResult(
                        "QQQ",
                        new ArrayList<>(Arrays.asList(new ClickStreamRecommendationResult[]{
                                new ClickStreamRecommendationResult("CPA link", 3.0761422E7, 10),
                                new ClickStreamRecommendationResult("CPA nolink", 3.0761422E7, 20)
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

//        for(ClickStreamResult r: job.output) {
//            System.out.println(r);
//        }

        assertTrue("Needles not found", job.output.containsAll(needles));
    }

    @Test
    public void testMultiLanguageClickStreamEvaluation() throws Exception {
//        ClickStreamEvaluation job = new ClickStreamEvaluation();

//        job.start("--wikisim " + resource("wikisim_output_lang_simple.csv")
//                + " --gold " + resource("clickstream.tsv")
//                + "," + resource("clickstream_2.tsv") // Multiple inputs
//                + " --lang simple"
//                + " --langlinks " + resource("wikisim_output_lang_simple.csv")
//                + " --output local");

        ClickStreamEvaluation job = new ClickStreamEvaluation();

        job.enableLocalEnvironment().silent().start("--wikisim " + wikiSimLangSimplePath
                + " --gold " + dataSetPathSimpleLang
                + " --lang simple"
                + " --langlinks " + langLinksPath
                + " --output local");
        String langPrefix = "simple_";

        // Needles
        ArrayList<ClickStreamResult> needles = new ArrayList<>(
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

        for(ClickStreamResult r: job.output) {
            System.out.println(r);
        }

        assertTrue("Needles not found", job.output.containsAll(needles));
    }
}
