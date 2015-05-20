package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.linkgraph.LinkGraph;
import org.junit.Test;

/**
 * Created by malteschwarzer on 04.12.14.
 */
public class LinkGraphTest {
    @Test
    public void TestLocal() throws Exception {
        String inputWikiFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso2.xml").nextElement().getPath();
        String inputLinkTuplesFilename = "file://" + getClass().getClassLoader().getResources("linkGraphInput.csv").nextElement().getPath();

        String inputRedirects = "file://" + getClass().getClassLoader().getResources("redirects.out").nextElement().getPath();

        String outputFilename = "print"; //"file://" + getClass().getClassLoader().getResources("linkgraph.out").nextElement().getPath();

        LinkGraph.main(new String[]{inputWikiFilename, inputRedirects, inputLinkTuplesFilename, outputFilename});
    }
}
