package org.wikipedia.citolytics.tests;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.junit.Test;
import org.wikipedia.citolytics.multilang.LangLinkTuple;
import org.wikipedia.citolytics.multilang.MultiLang;
import org.wikipedia.citolytics.tests.utils.Tester;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MultiLangTest extends Tester {
    @Test
    public void testReadLangLinksDataSet() throws Exception {
        ExecutionEnvironment env = new LocalEnvironment();

        List<LangLinkTuple> data = MultiLang.readLangLinksDataSet(env, resource("langlinks.sql")).collect();

        assertEquals("Invalid number of links from langlinks.sql", 90548, data.size());

    }

    @Test
    public void testClickStreamLangLinks() throws Exception {
        ExecutionEnvironment env = new LocalEnvironment();

        List<LangLinkTuple> data = MultiLang.readLangLinksDataSet(env, resource("ClickStreamTest/lang_links_enwiki.sql")).collect();

        assertEquals("Invalid number of lang links from sql dump",4, data.size());
    }
}