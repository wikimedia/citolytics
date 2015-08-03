package de.tuberlin.dima.schubotz.wikisim.cpa.tests;

import de.tuberlin.dima.schubotz.wikisim.cpa.tests.utils.Tester;
import de.tuberlin.dima.schubotz.wikisim.linkgraph.LinkGraph;
import org.junit.Ignore;
import org.junit.Test;


public class LinkGraphTest extends Tester {
    @Ignore
    @Test
    public void TestLocal() throws Exception {

        LinkGraph.main(new String[]{
                resource("wikiSeeAlso2.xml"),
                resource("redirects.out"),
                resource("linkGraphInput.csv"),
                "print"
        });
    }
}
