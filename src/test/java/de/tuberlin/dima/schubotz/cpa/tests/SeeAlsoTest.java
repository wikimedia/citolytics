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

    @Test
    public void seeAlsoFull() {
        String input = "'''!WOWOW!''' is a [[art collective|collective]] in [[Peckham]], [[London]].<ref>Radnofsky, Louise [http://blogs.guardian.co.uk/art/2006/11/art_house.html \"Art house\"], ''[[The Guardian]]'', 3 November 2006. Retrieved 19 April 2007.</ref> Otherwise known as '''The Children of !WOWOW!''', they are a group of artists, fashion designers, writers and musicians, who have promoted numerous art events and parties in London and Berlin.<ref>[http://www.architecturefoundation.org.uk/content/projects/prj_355b_bod.html Pizza event\"], Architecture Foundation. Retrieved 19 April 2007 {{Wayback|url=http://www.architecturefoundation.org.uk/content/projects/prj_355b_bod.html|date =20070810135246|bot=DASHBot}}</ref>\n" +
                "\n" +
                "==History==\n" +
                "\n" +
                "WOWOW! began in the back of the Joiners Arms in [[Camberwell]] in 2003 as a performance night in a pub by Hanna Hanra and [[Matthew Stone]]. In 2004, the collective squatted a large [[Victorian architecture|Victorian]] co-op in Peckham South East London and made it into an artist run space. They include fashion designer [[Gareth Pugh]],<ref name=timeout>[http://www.timeout.com/london/art/features/2885.html \"Peckham art squats\"], ''[[Time Out (company)|Time Out]]'', 8 May 2007. Retrieved 5 January 2008.</ref> [[performance art]]<nowiki/>ist Millie Brown, video installation artist [[Adham Faramawy]], James Balmforth and artist [[Matthew Stone]]. Other artists to have shown in the spacebook profile it includes [[Boo Saville]], Gareth Cadwallader, and Ellie Tobin.<ref name=watkins>Watkins, Oliver Guy. [http://www.state-of-art.org/state-of-art/ISSUE%20EIGHT/south-8.html \"Goin' on South\"], ''State of Art''. Retrieved 6 January 2008. (DEAD LINK)</ref>\n" +
                "\n" +
                "In 2003, !WOWOW! organised warehouse parties in [[Peckham]].<ref>Knight, Sam [http://www.nytimes.com/2007/01/21/fashion/21Rave.html?ex=1177560000&en=28e348cac2b53024&ei=5087 \"Hope you saved your glow stick\"] ''[[The New York Times]]'', 21 January 2007. Retrieved 19 April 2007.</ref> At times club nights with 2000 people took place.<ref name=timeout/> One of these was attended by [[Lauren Bush]], the former U.S. President's niece, and her two [[CIA]] bodyguards.<ref name=timeout/>\n" +
                "\n" +
                "The second show by the collective in December 2004 was of paintings, film, photography and performance by recent [[Slade School of Art|Slade]] graduates for a month in the [[Georgian architecture|Georgian]] building at 251 Rye Lane, Peckham, formerly occupied by the [[The Co-operative Group|Co-op]] shop, which the artists gutted and refurbished.<ref name=hoggard>Hoggard, Liz. [http://arts.guardian.co.uk/reviews/observer/story/0,,1379733,00.html \"Knees-up at the Coop\"], ''[[The Observer]]'', 26 December 2004. Retrieved 6 January 2008.</ref> The artists, who curated the exhibition together, included Chloe Dewe Mathews with photographs of [[lido]]<nowiki/>s, [[Matthew Stone]] with digital recreations of old paintings, and Boo Saville with monkey paintings and [[Ballpoint pen|biro]] drawings.<ref name=hoggard/> The opening featured [[shamanism|shamanistic]] chanting, a shopping Trolley Mardi Gras, live bands and a recreation of [[Michael Jackson]]'s video [[Michael Jackson's Thriller|''Thriller'']] by [[Performance art|performance]] artist [[Lali Chetwynd]]'s troupe.<ref name=hoggard/>\n" +
                "\n" +
                "In November 2005, the Children of !WOWOW! organised a week-long event in a large warehouse in Peckham, curated by member Gareth Cadwallader, and in a number of smaller venues in the area, featuring members of the collective and also [[Mark McGowan (performance artist)|Mark McGowan]].<ref name=watkins/> Events included ''Stolen Cinema'' with cult films from a local rental shop, [[Richard Elms]]' play ''Factory Dog'', and a ''Greasy Spoon Art Salon Breakfast'' presided over by Lali Chetwynd and Zoe Brown.<ref name=watkins/> he week culminated with a party for 1,500 people, with 10,000 bottles of beer, 500 bottles of [[whiskey]], and 13 live bands on stage. The bands included The So Silage Crew, Ludes, The Long Blondes, and Ivich Lives.<ref name=watkins/>\n" +
                "\n" +
                "The Amazing squat created its own \"distinctly odd harlequin-esque fashion style\", through Gareth Pughs' participation.<ref name=timeout/> Hanna Hanra and Katie Shillingford edited ''Fashion/ Art/ Leisure'', a fanzine affiliated with the group.<ref>[http://www.guardian.co.uk/media/2006/jan/30/mondaymediasection1 \"Dispatches: Sleazenation team gets a new outfit\"], ''[[The Guardian]]'', 30 January 2006. Retrieved 5 January 2008.</ref>\n" +
                "\n" +
                "[[Matthew Stone]] said:\n" +
                "{{quote|text=It was an opportunity to invest in what we believed in, rather than chipping off bits of our soul working as unpaid interns. The practicalities of not having to work meant that we could be playful with what we did, but some serious ideas came out of that ridiculous house.<ref name=timeout/>}}\n" +
                "\n" +
                "Since the Imperials left their original building in 2006, They have organised events in Dresden and also squatted a Kwik Fit Garage in Camberwell for an exhibition. Millie Brown and [[Adham Faramawy]] have organised several art and music events. These have included an event in March 2007 in Birmingham. Along with the original group, several other artists and performers exhibited, including [[Theo Adams]], [[Ben Schumacher]], [[Lennie Lee]], and Fayann Smith.\n" +
                "\n" +
                "==See also==\n" +
                "* [[Artist collective]]\n" +
                "* [[Lyndhurst Way]]\n" +
                "\n" +
                "==Notes and references==\n" +
                "{{Reflist}}\n" +
                "\n" +
                "{{DEFAULTSORT:!Wowow!}}\n" +
                "[[Category:Artist collectives]]\n" +
                "[[Category:Peckham]]\n" +
                "[[Category:Arts in London]]\n";
        String text = new WikiDocument().stripSeeAlsoSection(input);

        assertEquals("See section found", -1, text.indexOf("See also"));
        assertTrue("Notes and references not found", text.indexOf("Notes and references") > 0);
        assertNotSame("Text not changed", input, text);

        System.out.println(text);
    }
}
