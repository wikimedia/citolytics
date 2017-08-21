package org.wikipedia.citolytics.tests;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.edits.EditEvaluation;
import org.wikipedia.citolytics.edits.EditRecommendationExtractor;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.io.FileNotFoundException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class EditTest extends Tester {
    EditRecommendationExtractor job;
    String historyDumpPath;

    @Before
    public void before() throws FileNotFoundException {
//        wikiSimPath = resource("wikisim_output.csv", true);
        historyDumpPath = resource("stub-meta-history.xml",true);
//        outputPath = resource("citolytics.json.out",true);
//        missingIdsPath = resource("missing_ids.xml",true);
//        articleStatsPath = resource("stats.in", true);

        job = new EditRecommendationExtractor();
        job.enableTestEnvironment();
    }

    @Test
    public void testExtractRecommendations() throws Exception {

        job.start("--input " + historyDumpPath + " --output print");
    }

    @Test
    public void testExtractRecommendationsWithFilter() throws Exception {

        job.start("--input " + historyDumpPath + " --articles \"Bülg (Physik)\" --output local");
//        job.start("--input " + historyDumpPath + " --articles Bewick --output local");

//        System.out.println(getJobOutputAsString(job));
        assertEquals("Invalid output", "(Bülg (Physik),[(Chemisch Reaktschon,2.0,0), (Bewick,1.0,0), (Book,1.0,0), (Bull,1.0,0)],0)", getJobOutputAsString(job));
//        assertEquals("Invalid output", "(Bewick,[(Aanten,1.0,0), (Adam vun Bremen,1.0,0), (Amerika,1.0,0), (Anatolien,1.0,0), (Anion,1.0,0), (Ankara,1.0,0), (Asien,1.0,0), (Astronomie,1.0,0)],0)", getJobOutputAsString(job));
    }

    @Test
    public void testSimpleEvaluation() throws Exception {
        EditEvaluation job = new EditEvaluation();
        job.enableTestEnvironment();

//        job.start();
    }

    @Ignore
    @Test
    public void testRegex() {
        String revXml = "<revision>\n" +
                "      <id>4442</id>\n" +
                "      <parentid>4435</parentid>\n" +
                "      <timestamp>2004-07-25T11:21:27Z</timestamp>\n" +
                "      <contributor>\n" +
                "        <username>Suisui</username>\n" +
                "        <id>55</id>\n" +
                "      </contributor>\n" +
                "      <minor/>\n" +
                "      <comment>+:is fix:sr</comment>\n" +
                "      <model>wikitext</model>\n" +
                "      <format>text/x-wiki</format>\n" +
                "      <text id=\"4442\" bytes=\"2183\" />\n" +
                "      <sha1>osv0e0ylhrmj4y54ji38ssqacmh6qqx</sha1>\n" +
                "    </revision>";

        Pattern commentRegex = Pattern.compile("<comment>(.*?)</comment>", Pattern.DOTALL);
        Pattern contributorRegex = Pattern.compile("<contributor>(\\s+)<username>(.*?)</username>(\\s+)<id>(.*?)</id>(\\s+)</contributor>", Pattern.DOTALL);

        Matcher m = commentRegex.matcher(revXml);
        m.find();
        System.out.println(m.group(1));

        Matcher m2 = contributorRegex.matcher(revXml);

        if(m2.find()) {
            System.out.println(m2.group(2));
            System.out.println(m2.group(4));


        }

    }
}
