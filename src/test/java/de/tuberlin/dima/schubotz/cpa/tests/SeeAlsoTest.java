package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Created by malteschwarzer on 13.11.14.
 */
public class SeeAlsoTest {

    @Test
    public void seeAlsoNotExists() {
        String input = "==Title==\nfooo\nbaaaar\n==Reference==\n* x\n* y";
        String text = new WikiDocument().stripSeeAlsoSection(input);


        assertEquals("See section found", -1, text.indexOf("See also"));
        assertEquals("Text changed", input, text);

    }

    @Test
    public void seeAlsoLastSection() {
        String input = "==Title==\nfooo\nbaaaar\n==Reference==\n* x\n* y\n==See also==\n* [[foo]]\n* [[bar]]";
        String text = new WikiDocument().stripSeeAlsoSection(input);

        assertEquals("See section found", -1, text.indexOf("See also"));
        assertNotSame("Text not changed", input, text);
    }

    @Test
    public void seeAlsoMiddleSection() {
        String input = "==Title==\nfooo\nbaaaar\n==See also==\n* [[foo]]\n* [[bar]]\n" +
                "==Reference==\n" +
                "* x\n" +
                "* y";
        String text = new WikiDocument().stripSeeAlsoSection(input);

        assertEquals("See section found", -1, text.indexOf("See also"));
        assertTrue("Reference not found", text.indexOf("Reference") > 0);
        assertNotSame("Text not changed", input, text);

        System.out.println(text);
    }
}
