package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.contracts.DocumentProcessor;
import de.tuberlin.dima.schubotz.cpa.stats.ArticleStats;
import de.tuberlin.dima.schubotz.cpa.stats.RedirectCount;
import de.tuberlin.dima.schubotz.cpa.stats.RedirectExtractor;
import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import org.junit.Test;

import java.io.InputStream;
import java.util.Scanner;

/**
 * Created by malteschwarzer on 27.01.15.
 */
public class ArticleStatsTest {
    @Test
    public void LocalExecution() throws Exception {
        String inputWikiFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso2.xml").nextElement().getPath();

        String outputFilename = "file://" + getClass().getClassLoader().getResources("articlestats.out").nextElement().getPath();


        ArticleStats.main(new String[]{
                "file://" + getClass().getClassLoader().getResources("wikiSeeAlso2.xml").nextElement().getPath(),
                "print" //outputFilename
        });
    }

    @Test
    public void RedirectionExecution() throws Exception {


        RedirectExtractor.main(new String[]{
                "file://" + getClass().getClassLoader().getResources("wikiRedirect.xml").nextElement().getPath(),
                "print" //outputFilename
        });
    }

    @Test
    public void RedirectionCount() throws Exception {


        RedirectCount.main(new String[]{
                "file://" + getClass().getClassLoader().getResources("linkGraphInput.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath(),

                "print" //outputFilename
        });
    }

    @Test
    public void HeadlineTest() {


        String xml = getFileContents("wikiSeeAlso.xml");

        WikiDocument doc = new DocumentProcessor().processDoc(xml);

        System.out.println("Headlines: " + doc.getHeadlines().size());

    }

    @Test
    public void AvgLinkDistanceTest() {


        String xml = getFileContents("wikiSeeAlso.xml");

        WikiDocument doc = new DocumentProcessor().processDoc(xml);

        System.out.println("AvgLinkDistance: " + doc.getAvgLinkDistance());

    }
    // Utilities

    private String getFileContents(String fname) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fname);
        Scanner s = new Scanner(is, "UTF-8");
        s.useDelimiter("\\A");
        String out = s.hasNext() ? s.next() : "";
        s.close();
        return out;
    }
}
