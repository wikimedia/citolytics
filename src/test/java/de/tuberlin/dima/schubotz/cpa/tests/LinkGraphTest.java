package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.LinkGraph;
import de.tuberlin.dima.schubotz.cpa.WikiSim;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by malteschwarzer on 04.12.14.
 */
public class LinkGraphTest {
    @Test
    public void TestLocal() throws Exception {
        String inputWikiFilename = "file://" + getClass().getClassLoader().getResources("wikiSeeAlso2.xml").nextElement().getPath();
        String inputLinkTuplesFilename = "file://" + getClass().getClassLoader().getResources("linkGraphInput.csv").nextElement().getPath();

        String outputFilename = "file://" + getClass().getClassLoader().getResources("linkgraph.out").nextElement().getPath();

        LinkGraph.main(new String[]{inputWikiFilename, inputLinkTuplesFilename, outputFilename});
    }
}
