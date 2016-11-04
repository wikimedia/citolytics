package org.wikipedia.citolytics.tests;

import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Ignore;
import org.junit.Test;
import org.wikipedia.citolytics.seealso.SeeAlsoExtractor;
import org.wikipedia.citolytics.tests.utils.Tester;
import org.wikipedia.processing.DocumentProcessor;

import java.util.ArrayList;
import java.util.regex.Pattern;

import static org.junit.Assert.*;


public class SeeAlsoTest extends Tester {

    @Ignore
    @Test
    public void LocalExecution() throws Exception {

        SeeAlsoExtractor.main(new String[]{resource("wikiSeeAlso2.xml"), "print"});
    }

    @Test
    public void ExtractSeeAlsoNoRedirects() throws Exception {
        SeeAlsoExtractor job = new SeeAlsoExtractor();

        job.start(("--input " + resource("wikiSeeAlso2.xml") + " --output local").split(" "));

//        System.out.println(job.output);
//        System.out.println(job.output.size());

        // Needles
        ArrayList<Tuple3<String, String, Integer>> needles = new ArrayList<>();

        needles.add(new Tuple3<>("ASCII", "3568 ASCII#ASCII art#ASCII Ribbon Campaign#Extended ASCII#HTML decimal character rendering", 5));
        needles.add(new Tuple3<>("Algeria", "Index of Algeria-related articles#Outline of Algeria", 2));
        needles.add(new Tuple3<>("NFL on NBC", "List of NFL on NBC commentator pairings#List of AFL Championship Game broadcasters#List of NFL Championship Game broadcasters#List of AFC Championship Game broadcasters#List of NFC Championship Game broadcasters#List of Super Bowl broadcasters#NFL on NBC Radio#NFL on NBC music", 8));
        needles.add(new Tuple3<>("Africa Squadron", "Artist collective#Lyndhurst Way", 2)); // indent line

        assertTrue("Needles not found", job.output.containsAll(needles));

        // Size
        assertEquals("SeeAlso output count wrong.", 7, job.output.size());
    }

    @Test
    public void ExtractSeeAlsoWithRedirects() throws Exception {
        SeeAlsoExtractor job = new SeeAlsoExtractor();

        job.start(("--input " + resource("wikiSeeAlso2.xml") + " --output local --redirects " + resource("redirects.csv")).split(" "));


        // Needles (random link order)
        // ASCII art -> ASCII Art
        int found = 0;
        for (Tuple3<String, String, Integer> r : job.output) {
            if (r.f0.equals("ASCII")) {
                if (r.f2 == 5 && r.f1.indexOf("ASCII Art") > -1) {
                    found++;
                }
            }
        }

        assertEquals("Needles not found", 1, found);

        // Size
        assertEquals("SeeAlso output count wrong.", 7, job.output.size());
    }


    @Test
    public void seeAlsoNotExists() {
        String input = "==Title==\nfooo\nbaaaar\n==Reference==\n* x\n* y";
        String text = DocumentProcessor.stripSeeAlsoSection(input);


        assertEquals("See section found", -1, text.indexOf("See also"));
        assertEquals("Text changed", input, text);

    }

    @Test
    public void seeAlsoLastSection() {
        String input = "==Title==\nfooo\nbaaaar\n==Reference==\n* x\n* y\n==See also==\n* [[foo]]\n* [[bar]]";
        String text = DocumentProcessor.stripSeeAlsoSection(input);

        assertEquals("See section found", -1, text.indexOf("See also"));
        assertNotSame("Text not changed", input, text);
    }

    @Test
    public void seeAlsoMiddleSection() {
        String input = "==Title==\nfooo\nbaaaar\n==See also==\n* [[foo]]\n* [[bar]]\n" +
                "==Reference==\n" +
                "* x\n" +
                "* y";
        String text = DocumentProcessor.stripSeeAlsoSection(input);

        assertEquals("See section found", -1, text.indexOf("See also"));
        assertTrue("Reference not found", text.indexOf("Reference") > 0);
        assertNotSame("Text not changed", input, text);

        //System.out.println(text);
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
        String text = DocumentProcessor.stripSeeAlsoSection(input);

        assertEquals("See section found", -1, text.indexOf("See also"));
        assertTrue("Notes and references not found", text.indexOf("Notes and references") > 0);
        assertNotSame("Text not changed", input, text);

//        System.out.println(text);
    }

    @Test
    public void beginningOfLine() throws Exception {
        String content =
                "===2010s===\n" +
                        "As of the [[2014 NFL season|2014 season]], ''Football Night in America'' is hosted by [[Bob Costas]], who hosts from the game site, with [[Dan Patrick]] emceeing from the [[Stamford, Connecticut]] studio. [[Tony Dungy]] and [[Rodney Harrison]] are studio analysts. ''[[Sports Illustrated]]'' reporter [[Peter King (sportswriter)|Peter King]] serves as a feature reporter.  ''FNIA'' was broadcast from Studio 8G (and then from Studio 8H) from the GE (now Comcast) Building at 30 Rockefeller Plaza in New York from 2006-2013 before its own relocation to Stamford in September 2014, joining all of NBC Sports' other operations and [[NBCSN]].\n" +

                        "On December 14, 2011, the NFL, along with Fox, NBC and CBS, announced the league's rights deal with all three networks was extended to the end of the 2022 season. The three network rights deal includes the continued rotation of the Super Bowl yearly among the three, meaning NBC will air Super Bowls [[Super Bowl XLIX|XLIX]] (2015), [[Super Bowl LII|LII]] (2018), and LV (2021).<ref>{{cite news|url=http://blog.chron.com/ultimatetexans/2011/12/nfl-extends-broadcast-agreements-through-2022-generating-billions/|title=NFL extends broadcast agreements through 2022, generating billions|last=Barron|first=David|date=December 14, 2011|work=Houston Chronicle|accessdate=December 19, 2011}}</ref> The new rights deal also includes NBC receiving the primetime game of the Thanksgiving tripleheader previously carried by NFL Network, along with a division playoff game and one wild card game rather than the full Wild Card Saturday package.\n" +
                        "\n" +
                        "NBC's broadcast of [[Super Bowl XLVI]] at the end of the [[2011 NFL season|2011 season]] became the most-watched program in the history of [[Television in the United States|United States television]], with 111.3 million US viewers, according to [[Nielsen Company|Nielsen]].<ref>{{cite web|first=David |last=Bauder |url=http://www.huffingtonpost.com/2012/02/06/super-bowl-ratings-record-tv-giants-patriots_n_1258107.html |title=Super Bowl Ratings Record: Giants-Patriots Game Is Highest-Rated TV Show In US History |work=Huffington Post |date=February 6, 2012 |accessdate=February 7, 2012}}</ref>\n" +
                        "\n" +
                        "Sunday Night Football was the most watched program in the United States in the 2011-12 season and again in the 2013-14 season, in the latter case, NBC finished the season as the #1 network among 18-49 year olds for the first time since 2004 and #2 in overall viewers behind longtime leader CBS.<ref>{{cite web | url=http://www.thefutoncritic.com/ratings/2014/05/20/nbc-wins-the-2013-14-september-to-may-primetime-television-season-702212/20140520nbc04/ | title=NBC Wins the 2013-14 September-to-May Primetime Television Season | work=The Futon Critic | date=May 20, 2014 | accessdate=September 8, 2014}}</ref>\n" +
                        "\n" +
                        "====See also====\n" +
                        "*[[Super Bowl XLIII]] – As previously mentioned, Super Bowl XLIII was NBC's first Super Bowl broadcast since [[Super Bowl XXXII]] at the end of the [[1997 NFL season|1997 season]],<ref>{{cite web|url=http://www.cnbc.com/id/28898650/site/14081545/for/cnbc/ |title=NBC says Super Bowl ad sales nearly done – News |publisher=CNBC.com |date=January 28, 2009 |accessdate=February 4, 2009}} {{Dead link|date=September 2010|bot=H3llBot}}</ref> and was available in [[1080i]] [[high definition television|high definition]].  [[Play-by-play]] announcer [[Al Michaels]] and [[color commentator]] [[John Madden]] were in the booth, with [[Andrea Kremer]] and [[Alex Flanagan]] serving as [[sideline reporter]]s. The pre-game show – a record five hours long – was hosted by the ''[[Football Night in America]]'' team headed by [[Bob Costas]], and preceded by a two-hour special edition of ''[[Today (NBC program)|Today]]'' hosted by the regular weekday team live from Tampa and the [[NFL Films]] – produced ''Road to the Super Bowl''. [[Matt Millen]] was part of the coverage as a studio analyst. The'' Today'' contribution included portions of a taped interview with President Obama and pictures of troops viewing the proceedings in Iraq. John Madden was the first person to have announced a Super Bowl for each of the four major U.S. television networks, having called five Super Bowls for [[NFL on CBS|CBS]], three for [[NFL on Fox|Fox]], and two for [[Monday Night Football|ABC]] prior to joining NBC in [[2006 NFL season|2006]]. Meanwhile, Al Michaels was the third man to do play-by-play for a Super Bowl on NBC television (following in the footsteps of [[Curt Gowdy]] and [[Dick Enberg]]). Also, Michaels became the second person (after [[Pat Summerall]] on [[NFL on CBS|CBS]] and [[NFL on Fox|Fox]]) to be the lead Super Bowl play-by-play announcer for two different major U.S. networks ([[Monday Night Football|ABC]] and [[NBC Sunday Night Football|NBC]]). This would prove to be the final game Madden would call, as he announced his retirement from broadcasting on April 16, 2009. The Super Bowl was one of two major professional sports championship series [[NBC Sports|NBC]] broadcast in 2009, as they would also broadcast the [[2009 Stanley Cup Finals|Stanley Cup Finals]]. Both championship series involved teams from Pittsburgh winning championships.<ref name=StanleyCupFinals/> [[Mike Emrick]], [[Ed Olczyk]], and [[Pierre McGuire]] mentioned this when they called the Stanley Cup Finals.<ref name=StanleyCupFinals>{{cite video|title=[[NHL on NBC]]: Game 7 of the 2009 Stanley Cup Finals|medium=television|publisher=NBC Sports|date=June 12, 2009}} Emrick, Olczyk, and McGuire mentioned about Pittsburgh having two championships in the same year, as the [[Pittsburgh Penguins|Penguins]] won the Stanley Cup.</ref> Super Bowl XLIII was the final Super Bowl to air in the [[analog television]] format in the United States before the [[DTV transition in the United States|nationwide digital television transition]]. The transition, originally scheduled for February 17 was pushed back to June 12, the same day the Penguins won the Stanley Cup.\n" +
                        "\n" +
                        "*[[Super Bowl XLVI]] – It was also [[Streaming media|streamed]] live online, both to computers (via NFL.com and NBCSports.com) and mobile devices (via [[Verizon Wireless]]'s NFL Mobile [[Application software|app]]), the first legal online streaming of a Super Bowl telecast in the USA.<ref>{{cite news\n" +
                        "|url=http://abcnews.go.com/blogs/technology/2011/12/super-bowl-will-be-live-streamed-online-for-first-time/\n" +
                        "|title=Super Bowl Will Be Live-Streamed Online for First Time|publisher=[[ABC News]]|work=technology Review|first=Lauren|last=Effron|date=December 20, 2011|accessdate=January 4, 2012}}</ref><ref>{{cite news |url=http://www.cnn.com/2011/12/20/tech/web/super-bowl-online/index.html?hpt=hp_t3|title=NFL playoffs, Super Bowl to be streamed online|publisher=CNN|work=CNN Tech|first=Mark|last=Milian|date=December 20, 2011|accessdate=January 4, 2012}}</ref> [[Al Michaels]] called [[play-by-play]] for NBC.<ref>{{cite news| url=http://www.usatoday.com/sports/2005-04-18-nbc-abc_x.htm | work=USA Today | first1=Michael | last1=Hiestand | title=ESPN gets 'MNF' | date=April 19, 2005}}</ref> marking the eighth time that he was behind the microphone for a Super Bowl and the second time he called a Super Bowl for NBC (Michaels had previously done play-by-play for Super Bowls [[Super Bowl XXII|XXII]], [[Super Bowl|XXV]], [[XXIX]], [[XXXIV]], [[Super Bowl XXXVII|XXXVII]], and [[Super Bowl XL|XL]] for [[Monday Night Football|ABC]] and [[Super Bowl XLIII]] for NBC). [[Cris Collinsworth]] was the [[color analyst]] for the game, his second Super Bowl as a game analyst and first since he was in the booth for [[Super Bowl XXXIX]] for [[NFL on Fox|Fox]]. [[Michele Tafoya]] was the [[sideline reporter]]. [[Bob Costas]] and [[Dan Patrick]] (who also presided over the trophy presentation ceremony) hosted the pregame, halftime, and postgame coverage for NBC with ''[[Football Night in America]]'' analysts [[Tony Dungy]] and [[Rodney Harrison]] and special guest analysts (who were seated next to Costas during the pre-game festivities), [[Aaron Rodgers]] and [[Hines Ward]]. Also helping out on NBC's broadcast were reporters [[Alex Flanagan]] and [[Randy Moss (sports reporter)|Randy Moss]] and NFL insiders [[Mike Florio]] and [[Peter King (sportswriter)|Peter King]].\n" +
                        "\n" +
                        "==Pregame/Studio programs==\n" +
                        "{{main|The NFL on NBC pregame show|Football Night in America}}\n" +
                        "\n" +
                        "==Commentators==\n" +
                        "{{main|Football Night in America|NBC Sunday Night Football|List of NFL on NBC announcers|List of NFL on NBC pregame show panelists}}\n" +
                        "\n" +
                        "   ==See also==\n" +
                        "*[[List of NFL on NBC commentator pairings]]\n" +
                        "*[[List of AFL Championship Game broadcasters]]\n" +
                        "*[[List of NFL Championship Game broadcasters]]\n" +
                        "*[[List of AFC Championship Game broadcasters]]\n" +
                        "*[[List of NFC Championship Game broadcasters]]\n" +
                        "*[[List of Super Bowl broadcasters]]\n" +
                        "*''[[NFL on NBC Radio]]''\n" +
                        "*[[NFL on NBC music]]\n" +
                        "\n" +
                        "==References==\n" +
                        "{{reflist|2}}\n" +
                        "\n" +
                        "==External links==\n" +
                        "* {{official website|http://www.nbcsports.com/nfl/}}\n" +
                        "**[http://www.nbcsports.com/sundaynightfootball/index.html Sunday Night Football]\n" +
                        "* {{IMDb title|0407424}}\n" +
                        "\n" +
                        "{{Navboxes|list1=\n" +
                        "{{s-start}}\n" +
                        "[[Category:2010s American television series]]\n" +
                        "[[Category:National Football League television series|NBC]]\n" +
                        "[[Category:Television series revived after cancellation]]\n";

        if (Pattern.compile(DocumentProcessor.seeAlsoRegex, Pattern.MULTILINE + Pattern.CASE_INSENSITIVE).matcher(content).find()) {
            //System.out.println("See also section found.");
        } else {
            throw new Exception("See also not found if not in beginning of line.");
        }
    }
}
