package org.wikipedia.citolytics.tests;

import org.junit.Test;
import org.wikipedia.citolytics.multilang.LangLinkTuple;
import org.wikipedia.citolytics.multilang.MultiLang;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MultiLangTest extends Tester {
    @Test
    public void testReadLangLinksDataSet() throws Exception {

        List<LangLinkTuple> data = MultiLang.readLangLinksDataSet(getSilentEnv(), resource("MultiLangTest/langlinks.sql")).collect();

        assertEquals("Invalid number of links from langlinks.sql", 90548, data.size());

    }

    @Test
    public void testClickStreamLangLinks() throws Exception {

        List<LangLinkTuple> data = MultiLang.readLangLinksDataSet(getSilentEnv(), resource("ClickStreamTest/lang_links_enwiki.sql")).collect();

        assertEquals("Invalid number of lang links from sql dump",5, data.size());
    }

    @Test
    public void testSeeAlsoangLinks() throws Exception {

        List<LangLinkTuple> data = MultiLang.readLangLinksDataSet(getSilentEnv(), resource("SeeAlsoTest/lang_links.in"), "en").collect();

//        System.out.println(data);

        assertEquals("Invalid number of lang links from sql dump",6, data.size());

    }


}