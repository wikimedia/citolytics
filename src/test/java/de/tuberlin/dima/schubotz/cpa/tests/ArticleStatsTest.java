package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.ArticleStats;
import org.junit.Test;

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
                "file://" + getClass().getClassLoader().getResources("evaluation_links.csv").nextElement().getPath(),
                "file://" + getClass().getClassLoader().getResources("evaluation_seealso.csv").nextElement().getPath(),
                "print" //outputFilename
        });
    }
}
